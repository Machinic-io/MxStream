/*
 * Copyright 2026 Machinic.io LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.machinic.stream.spliterator;

import io.machinic.stream.MxStream;
import io.machinic.stream.StreamException;
import io.machinic.stream.StreamInterruptedException;
import io.machinic.stream.concurrent.MapFutureTask;
import io.machinic.stream.metrics.AsyncMapMetric;
import io.machinic.stream.metrics.AsyncMapMetricSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class AsyncMapSpliterator<IN, OUT> extends AbstractChainedSpliterator<IN, OUT> {
	
	private static final Logger logger = LoggerFactory.getLogger(AsyncMapSpliterator.class);
	
	private final Supplier<Function<? super IN, ? extends OUT>> supplier;
	private final Function<? super IN, ? extends OUT> mapper;
	private final int parallelism;
	private final long asyncTimeoutMillis;
	private final ExecutorService providedExecutorService;
	private final ExecutorService executorService;
	private final AsyncMapMetricSupplier metricSupplier;
	private final AsyncMapMetric metric;
	private final Queue<MapFutureTask<IN, OUT>> queue;
	private boolean started;
	
	public AsyncMapSpliterator(MxStream<IN> stream, MxSpliterator<IN> previousSpliterator, int parallelism, long asyncTimeoutMillis, ExecutorService executorService, AsyncMapMetricSupplier metricSupplier, Supplier<Function<? super IN, ? extends OUT>> supplier) {
		super(stream, previousSpliterator);
		this.supplier = supplier;
		this.mapper = supplier.get();
		this.parallelism = parallelism;
		this.asyncTimeoutMillis = asyncTimeoutMillis;
		this.providedExecutorService = executorService;
		this.executorService = (providedExecutorService != null ? providedExecutorService : Executors.newVirtualThreadPerTaskExecutor());
		this.metricSupplier = metricSupplier;
		this.metric = (metricSupplier != null ? metricSupplier.get() : null);
		// the queue does not need to be thread-safe as it is only accessed by the stream thread
		this.queue = new ArrayDeque<>(parallelism + 2);
	}
	
	private void enqueue(MapFutureTask<IN, OUT> mapFutureTask) {
		try {
			this.executorService.submit(mapFutureTask);
			if (!queue.offer(mapFutureTask)) {
				throw new StreamException("Failed to enqueue task. Queue is full");
			}
		} catch (RejectedExecutionException e) {
			throw new StreamException("Failed to enqueue task. Caused by RejectedExecutionException", e);
		} catch (StreamException e) {
			throw e;
		} catch (Exception e) {
			throw new StreamException(String.format("Failed to enqueue task. Caused by %s", e.getMessage()), e);
		}
	}
	
	private void dequeue(Consumer<? super OUT> action, boolean drain) throws StreamException {
		MapFutureTask<IN, OUT> futureTask = queue.peek();
		if (futureTask != null) {
			if (futureTask.isDone() || this.getQueueSize() >= parallelism || drain) {
				try {
					// Check and clear the interrupted status
					if (Thread.interrupted()) {
						throw new StreamInterruptedException("Stream has been interrupted");
					}
					long startTimestamp = System.nanoTime();
					try {
						if (futureTask.await(this.asyncTimeoutMillis, TimeUnit.MILLISECONDS)) {
							// Task is in a done state, we can safely dequeue the task
							queue.poll();
							if (futureTask.getException() == null) {
								action.accept(futureTask.getResult());
							} else {
								try {
									getStream().exceptionHandler().onException(futureTask.getException(), futureTask.getInput());
								} catch (StreamException e) {
									throw e;
								} catch (Exception e) {
									throw new StreamException(String.format("asyncMap failed. Caused by %s", e.getMessage()), e);
								}
							}
							
							if (metric != null) {
								metric.onEvent(futureTask.getPendingDuration(), futureTask.getDuration());
							}
						} else {
							logger.warn("asyncMap task timed out, cancelling task for {}", futureTask.getInput());
							// Task will be canceled, we can safely dequeue the task
							queue.poll();
							futureTask.cancel(true);
							getStream().exceptionHandler().onException(new StreamInterruptedException("asyncMap has been interrupted"), futureTask.getInput());
						}
					} catch (InterruptedException e) {
						throw new StreamInterruptedException("Stream has been interrupted");
					} finally {
						if (metric != null) {
							long endTimestamp = System.nanoTime();
							metric.onWait(endTimestamp - startTimestamp);
						}
					}
					
				} catch (StreamException e) {
					while ((futureTask = queue.poll()) != null) {
						futureTask.cancel(true);
					}
					throw e;
				} catch (Exception e) {
					throw new StreamException(String.format("mapAsync failed. Caused by %s", e.getMessage()), e);
				}
			}
		}
	}
	
	private int getQueueSize() {
		return queue.size();
	}
	
	@Override
	public boolean tryAdvance(Consumer<? super OUT> action) {
		if (!started && this.metric != null) {
			this.metric.onStart();
			started = true;
		}
		
		boolean canAdvance;
		do {
			canAdvance = this.previousSpliterator.tryAdvance(value ->
			{
				this.enqueue(new MapFutureTask<IN, OUT>(this.mapper, value));
				dequeue(action, false);
			});
		} while (canAdvance && this.getQueueSize() <= parallelism);
		
		if (!canAdvance) {
			do {
				dequeue(action, true);
			} while (this.getQueueSize() != 0);
		}
		
		return canAdvance;
	}
	
	@Override
	public AbstractChainedSpliterator<IN, OUT> split(MxSpliterator<IN> spliterator) {
		return new AsyncMapSpliterator<>(stream, spliterator, parallelism, asyncTimeoutMillis, this.providedExecutorService, metricSupplier, supplier);
	}
	
	@Override
	public void close() {
		if (this.metric != null) {
			this.metric.onStop();
		}
		
		// shutdown self-created executor service
		if(this.providedExecutorService == null && this.executorService != null) {
			this.executorService.shutdown();
		}
		
		MapFutureTask<IN, OUT> futureTask;
		while ((futureTask = queue.poll()) != null) {
			futureTask.cancel(true);
		}
		
	}
}
