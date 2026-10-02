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

package io.machinic.stream;

import io.machinic.stream.spliterator.BlockingQueueReaderSpliterator;
import io.machinic.stream.util.Require;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

public class TapBuilder<T> {
	
	protected final int capacity;
	protected final BlockingQueue<BlockingQueueReaderSpliterator.QueueWrapper<T>> queue;
	private volatile BasePipeline<?, T> source;
	private volatile boolean parallel = false;
	private volatile int parallelism = ForkJoinPool.getCommonPoolParallelism();
	private volatile ExecutorService executorService = ForkJoinPool.commonPool();
	private final CountDownLatch latch = new CountDownLatch(1);
	
	public TapBuilder(int capacity) {
		this.capacity = capacity;
		this.queue = new ArrayBlockingQueue<>(capacity);
	}
	
	public TapBuilder<T> parallel(boolean parallel) {
		this.parallel = parallel;
		return this;
	}
	
	public TapBuilder<T> parallelism(int parallelism) {
		Require.equalOrGreater(parallelism, 1, "parallelism");
		this.parallelism = parallelism;
		return this;
	}
	
	public TapBuilder<T> executorService(ExecutorService executorService) {
		Require.equalOrGreater(parallelism, 1, "parallelism");
		this.executorService = executorService;
		return this;
	}
	
	protected TapBuilder<T> source(BasePipeline<?, T> parentPipeline) {
		this.source = parentPipeline;
		this.latch.countDown();
		return this;
	}
	
	public MxStream<T> build() {
		BasePipeline<?, T> source = this.source;
		Objects.requireNonNull(source, "Not attached to stream");
		return new PipelineSource.TapSource<>(source, this.queue, this.parallel, this.parallelism, executorService);
	}
	
	/**
	 * Block until builder has been attached to MxStream and then return tap stream.
	 */
	public MxStream<T> awaitBuild() throws InterruptedException {
		this.latch.await();
		return build();
	}
	
}
