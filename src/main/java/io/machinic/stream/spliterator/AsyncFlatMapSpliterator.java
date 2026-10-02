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

import io.machinic.stream.FlatMapProducerFunction;
import io.machinic.stream.MxStream;
import io.machinic.stream.StreamException;
import io.machinic.stream.StreamInterruptedException;
import io.machinic.stream.concurrent.FlatMapFutureTask;
import io.machinic.stream.metrics.AsyncMapMetric;
import io.machinic.stream.metrics.AsyncMapMetricSupplier;
import io.machinic.stream.util.Wrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class AsyncFlatMapSpliterator<IN, OUT> extends AbstractChainedSpliterator<IN, OUT> {
	
	private static final Logger logger = LoggerFactory.getLogger(AsyncFlatMapSpliterator.class);
	
	private final Supplier<FlatMapProducerFunction<? super IN, ? extends OUT>> supplier;
	private final int parallelism;
	private final int bufferSize;
	private final long asyncTimeoutMillis;
	private final ExecutorService providedExecutorService;
	private final ExecutorService executorService;
	private final AsyncMapMetricSupplier metricSupplier;
	private final AsyncMapMetric metric;
	// This is only accessed by Spliterator thread, so it does not need to be thread safe
	private final Queue<FlatMapFutureTask<IN, OUT>> queue;
	private final ArrayBlockingQueue<Wrapper<OUT>> bufferQueue;
	private final int desiredCapacity;
	private final Consumer<OUT> advanceMapper;
	private boolean started;
	
	public AsyncFlatMapSpliterator(MxStream<IN> stream, MxSpliterator<IN> previousSpliterator, int parallelism, int bufferSize, long asyncTimeoutMillis, ExecutorService executorService, AsyncMapMetricSupplier metricSupplier,
			Supplier<FlatMapProducerFunction<? super IN, ? extends OUT>> supplier) {
		super(stream, previousSpliterator);
		this.supplier = supplier;
		this.parallelism = parallelism;
		this.bufferSize = bufferSize;
		this.asyncTimeoutMillis = asyncTimeoutMillis;
		this.providedExecutorService = executorService;
		this.executorService = (providedExecutorService != null ? providedExecutorService : Executors.newVirtualThreadPerTaskExecutor());
		this.metricSupplier = metricSupplier;
		this.metric = (metricSupplier != null ? metricSupplier.get() : null);
		// the queue does not need to be thread-safe as it is only accessed by the stream thread
		this.queue = new ArrayDeque<>(parallelism + 2);
		this.bufferQueue = new ArrayBlockingQueue<>(bufferSize);
		
		this.desiredCapacity = bufferSize / 4;
		
		this.advanceMapper = (value) -> {
			try {
				this.bufferQueue.put(new Wrapper<>(value));
			} catch (InterruptedException e) {
				throw new StreamInterruptedException(String.format("AsyncFlatMapSpliterator was interrupted while processing value %s", value), e);
			}
		};
		
	}
	
	private void enqueue(FlatMapFutureTask<IN, OUT> task) {
		try {
			this.executorService.submit(task);
			if (!queue.offer(task)) {
				throw new StreamException("Failed to enqueue flatMap task. Queue is full");
			}
		} catch (RejectedExecutionException e) {
			throw new StreamException("Failed to enqueue flatMap task. Caused by RejectedExecutionException", e);
		} catch (StreamException e) {
			throw e;
		} catch (Exception e) {
			throw new StreamException(String.format("Failed to enqueue flatMap task. Caused by %s", e.getMessage()), e);
		}
	}
	
	private void dequeue(Consumer<? super OUT> action, boolean drain) throws StreamException {
		FlatMapFutureTask<IN, OUT> futureTask = queue.peek();
		
		Wrapper<OUT> wrapper;
		do {
			wrapper = bufferQueue.poll();
			if (wrapper != null) {
				action.accept(wrapper.get());
			}
		} while (wrapper != null && (drain || (futureTask != null && !futureTask.isDone())));
		
		if (futureTask != null) {
			
			if (futureTask.isDone()) {
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
							if (futureTask.getException() != null) {
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
							logger.warn("asyncFlatMap task timed out, cancelling task for {}", futureTask.getInput());
							// Task will be canceled, we can safely dequeue the task
							queue.poll();
							futureTask.cancel(true);
							getStream().exceptionHandler().onException(new StreamInterruptedException("asyncFlatMap has been interrupted"), futureTask.getInput());
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
					throw new StreamException(String.format("asyncFlatMap failed. Caused by %s", e.getMessage()), e);
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
				this.enqueue(new FlatMapFutureTask<>(value, this.supplier.get(), this.advanceMapper));
				this.dequeue(action, false);
			});
		} while (canAdvance && this.getQueueSize() <= parallelism);
		
		dequeue(action, false);
		
		if (!canAdvance) {
			do {
				dequeue(action, true);
			} while (this.getQueueSize() != 0);
		}
		
		return canAdvance;
	}
	
	@Override
	public AbstractChainedSpliterator<IN, OUT> split(MxSpliterator<IN> spliterator) {
		return new AsyncFlatMapSpliterator<>(stream, spliterator, parallelism, bufferSize, asyncTimeoutMillis, this.providedExecutorService, metricSupplier, supplier);
	}
	
	@Override
	public void close() {
		if (this.metric != null) {
			this.metric.onStop();
		}
		
		// shutdown self-created executor service
		if (this.providedExecutorService == null && this.executorService != null) {
			this.executorService.shutdown();
		}
		
		FlatMapFutureTask<IN, OUT> futureTask;
		while ((futureTask = queue.poll()) != null) {
			futureTask.cancel(true);
		}
		
	}
	
}
