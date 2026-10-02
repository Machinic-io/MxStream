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

package io.machinic.stream.concurrent;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides functionality to create a {@code ThreadFactory} for {@code VirtualThread} that uses a custom carrier thread pool rather than {@code ForkJoinPool.commonPool()}. This allows much more control over the execution of the VirtualThreads since the
 * carrier thread pool can be created using additional options.
 * <P>In order for this to work this module must open java.base/java.lang. Modern JVM versions require this argument to allow access. {@code --add-opens=java.base/java.lang=io.machinic.stream}
 * <P>Note that VirtualThreads inherit some properties from the carrier thread, but return static values that the {@code ForkJoinPool.commonPool()} would have. {@code Thread.currentThread().getPriority()} is one such property.
 */
public class VirtualThreadFactory extends AbstractVirtualThreadFactory {
	
	private final String namePrefix;
	private final AtomicLong counter;
	private final Thread.UncaughtExceptionHandler uncaughtExceptionHandler;
	
	public VirtualThreadFactory(Executor carrierThreadPool, String namePrefix, long counterStart, boolean inheritInheritableThreadLocals, Thread.UncaughtExceptionHandler uncaughtExceptionHandler) throws ThreadFactoryException {
		super(carrierThreadPool, inheritInheritableThreadLocals);
		this.namePrefix = namePrefix;
		this.counter = new AtomicLong(counterStart);
		this.uncaughtExceptionHandler = uncaughtExceptionHandler;
	}
	
	/**
	 * Generate a new thread name
	 * @return the new thread name
	 */
	protected String nextName() {
		return String.format("%s-%s", this.namePrefix, counter.getAndIncrement());
	}
	
	@Override
	public Thread newThread(Runnable runnable) {
		Objects.requireNonNull(runnable);
		Thread thread = this.createVirtualThread(nextName(), runnable);
		if (this.uncaughtExceptionHandler != null) {
			thread.setUncaughtExceptionHandler(this.uncaughtExceptionHandler);
		}
		return thread;
	}
	
	/**
	 * Create new VirtualFixedThreadPool from this VirtualThreadFactory.
	 * @param theadPoolSize number of VirtualThreads in pool
	 * @return VirtualFixedThreadPool instance
	 */
	public ExecutorService newFixedThreadPool(int theadPoolSize) {
		return Executors.newFixedThreadPool(theadPoolSize, this);
	}
	
	/**
	 * Create new VirtualThreadPerTaskThreadPool from this VirtualThreadFactory.
	 * @return VirtualThreadPerTaskThreadPool instance
	 */
	public ExecutorService newThreadPerTaskExecutor() {
		return Executors.newThreadPerTaskExecutor(this);
	}
	
	/**
	 * Create VirtualThreadFactoryBuilder
	 * @return VirtualThreadFactoryBuilder
	 */
	public static VirtualThreadFactoryBuilder builder() {
		return new VirtualThreadFactoryBuilder();
	}
	
	/**
	 * Builder that builds a new VirtualThreadFactory instance.
	 */
	public static class VirtualThreadFactoryBuilder {
		private Executor carrierThreadPool = ForkJoinPool.commonPool();
		private String namePrefix = "VirtualThread";
		private long counterStart = 0L;
		private boolean inheritInheritableThreadLocals = false;
		private Thread.UncaughtExceptionHandler uncaughtExceptionHandler;
		
		public VirtualThreadFactoryBuilder carrierThreadPool(Executor carrierThreadPool) {
			this.carrierThreadPool = carrierThreadPool;
			return this;
		}
		
		public VirtualThreadFactoryBuilder namePrefix(String namePrefix) {
			this.namePrefix = namePrefix;
			return this;
		}
		
		public VirtualThreadFactoryBuilder counter(long counterStart) {
			this.counterStart = counterStart;
			return this;
		}
		
		public VirtualThreadFactoryBuilder inheritInheritableThreadLocals(boolean inheritInheritableThreadLocals) {
			this.inheritInheritableThreadLocals = inheritInheritableThreadLocals;
			return this;
		}
		
		public VirtualThreadFactoryBuilder uncaughtExceptionHandler(Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
			this.uncaughtExceptionHandler = uncaughtExceptionHandler;
			return this;
		}
		
		public VirtualThreadFactory build() {
			return new VirtualThreadFactory(carrierThreadPool, namePrefix, counterStart, inheritInheritableThreadLocals, uncaughtExceptionHandler);
		}
		
	}
	
}
