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
import io.machinic.stream.StreamTimeoutException;
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

/**
 * Spliterator implementation that performs asynchronous flat-map operations, executing the flat-map producer
 * function on a separate thread pool while buffering results. Used internally by the MxStream library to
 * implement the {@code asyncFlatMap} and {@code asyncFlatMapProducer} operations.
 *
 * <p>Each input element is submitted as an async task that produces zero or more output elements. Results are
 * buffered in a bounded queue and consumed in order. The number of concurrent tasks is limited by the configured
 * parallelism level.</p>
 *
 * @param <IN>  the type of elements consumed from the previous stage
 * @param <OUT> the type of elements produced by this stage
 */
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
	private final Consumer<OUT> advanceMapper;
	private boolean started;
	
	/**
	 * Creates a new async flat-map spliterator.
	 * @param pipeline the pipeline this spliterator belongs to
	 * @param previousSpliterator the spliterator from the previous stage
	 * @param parallelism the maximum number of concurrent async tasks
	 * @param bufferSize the buffer size for the async pipeline
	 * @param asyncTimeoutMillis the timeout for async operations in milliseconds
	 * @param executorService the executor for async tasks, or {@code null} for a virtual thread executor
	 * @param metricSupplier a supplier of async metrics, or {@code null}
	 * @param supplier the supplier of the flat-map producer function
	 */
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
		
		this.advanceMapper = (value) -> {
			try {
				this.bufferQueue.put(new Wrapper<>(value));
			} catch (InterruptedException e) {
				throw new StreamInterruptedException(String.format("AsyncFlatMapSpliterator was interrupted while processing value %s", value), e);
			}
		};
		
	}
	
	/**
	 * Submits an async flat-map task to the executor and tracks it in the task queue.
	 * @param task the flat-map future task to submit
	 * @throws StreamException if the task cannot be enqueued
	 */
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
	
	/**
	 * Dequeues buffered results and waits for the next completed task if necessary.
	 * @param action the action to apply to each dequeued element
	 * @param drain if {@code true}, drain all buffered results; otherwise only consume while the current task is not done
	 * @throws StreamException if an error occurs while waiting for or processing the task
	 */
	private void dequeue(Consumer<? super OUT> action, boolean drain) throws StreamException {
		FlatMapFutureTask<IN, OUT> futureTask = queue.peek();
		if (futureTask == null) {
			return;
		}
		
		if (!(futureTask.isDone() || this.getQueueSize() >= parallelism || drain)) {
			Wrapper<OUT> wrapper;
			while ((wrapper = bufferQueue.poll()) != null) {
				action.accept(wrapper.get());
			}
			return;
		}
		
		try {
			// Check and clear the interrupted status
			if (Thread.interrupted()) {
				throw new StreamInterruptedException("Stream has been interrupted");
			}
			long startTimestamp = System.nanoTime();
			try {
				long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(asyncTimeoutMillis);
				boolean completed = false;
				
				while (!completed) {
					long remainingNanos = deadline - System.nanoTime();
					if (remainingNanos <= 0) {
						break;
					}
					
					// Use a short poll timeout to interleave buffer consumption with task waiting,
					// preventing deadlock when the task blocks on a full bufferQueue.
					// A short timeout ensures we frequently check isDone() even when the buffer is empty.
					long pollTimeout = Math.min(remainingNanos, 1_000_000L);
					Wrapper<OUT> wrapper = bufferQueue.poll(pollTimeout, TimeUnit.NANOSECONDS);
					if (wrapper != null) {
						action.accept(wrapper.get());
					}
					
					if (futureTask.isDone()) {
						// Drain any remaining buffered results
						Wrapper<OUT> remaining;
						while ((remaining = bufferQueue.poll()) != null) {
							action.accept(remaining.get());
						}
						completed = true;
					}
				}
				
				if (completed) {
					queue.poll();
					if (futureTask.getException() != null) {
						try {
							getStream().exceptionHandler().onException(futureTask.getException(), futureTask.getInput());
						} catch (StreamException e) {
							throw e;
						} catch (Exception e) {
							throw new StreamException(String.format("asyncFlatMap failed. Caused by %s", e.getMessage()), e);
						}
					}
					
					if (metric != null) {
						metric.onEvent(futureTask.getPendingDuration(), futureTask.getDuration());
					}
				} else {
					logger.warn("asyncFlatMap task timed out, cancelling task for {}", futureTask.getInput());
					queue.poll();
					futureTask.cancel(true);
					getStream().exceptionHandler().onException(new StreamTimeoutException(String.format("asyncFlatMap task for element %s did not complete within %d ms", futureTask.getInput(), asyncTimeoutMillis)), futureTask.getInput());
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
	
	/**
	 * Returns the number of pending async tasks in the queue.
	 * @return the queue size
	 */
	private int getQueueSize() {
		return queue.size();
	}
	
	/**
	 * {@inheritDoc}
	 * @param action the action to apply to each flattened element
	 * @return {@code true} if an element was consumed
	 */
	@Override
	public boolean tryAdvance(Consumer<? super OUT> action) {
		if (!started && this.metric != null) {
			this.metric.onStart();
			started = true;
		}
		
		boolean canAdvance = true;
		while (this.getQueueSize() < parallelism) {
			canAdvance = this.previousSpliterator.tryAdvance(value ->
			{
				this.enqueue(new FlatMapFutureTask<>(value, this.supplier.get(), this.advanceMapper));
				this.dequeue(action, false);
			});
			if (!canAdvance) {
				break;
			}
		}
		
		dequeue(action, false);
		
		if (!canAdvance) {
			do {
				dequeue(action, true);
			} while (this.getQueueSize() != 0);
		}
		
		return canAdvance;
	}
	
	/**
	 * {@inheritDoc}
	 * @param spliterator the portion of the previous spliterator
	 * @return a new async flat-map spliterator for the split portion
	 */
	@Override
	public AbstractChainedSpliterator<IN, OUT> split(MxSpliterator<IN> spliterator) {
		return new AsyncFlatMapSpliterator<>(getStream(), spliterator, parallelism, bufferSize, asyncTimeoutMillis, this.providedExecutorService, metricSupplier, supplier);
	}
	
	/**
	 * {@inheritDoc}
	 * Stops the async metric (if present), shuts down the executor service (if self-created), and cancels
	 * all pending tasks.
	 */
	@Override
	public void close() {
		super.close();
		
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
