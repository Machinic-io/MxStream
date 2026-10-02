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

import java.lang.reflect.Constructor;
import java.lang.reflect.InaccessibleObjectException;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

/**
 * Provides functionality to create a {@code ThreadFactory} for {@code VirtualThread} that uses a custom carrier thread pool rather than {@code ForkJoinPool.commonPool()}. This allows much more control over the execution of the VirtualThreads since the carrier thread pool can be created using additional options.
 * <P>In order for this to work this module must open java.base/java.lang. Modern JVM versions require this argument to allow access. {@code --add-opens=java.base/java.lang=io.machinic.stream}
 * <P>Note that VirtualThreads inherit some properties from the carrier thread, but return static values that the {@code ForkJoinPool.commonPool()} would have. {@code Thread.currentThread().getPriority()} is one such property.
 */
public abstract class AbstractVirtualThreadFactory implements ThreadFactory {
	
	static final int INHERIT_THREAD_LOCALS = 0;
	static final int NO_INHERIT_THREAD_LOCALS = 4;
	
	private final Constructor<Thread> virtualThreadConstructor;
	private final Executor carrierThreadPool;
	private final int characteristics;
	
	/**
	 * Creates a new {@code ThreadFactor} with the provided Executor as the carrier thread pool.
	 * @param carrierThreadPool used as the carrier thread pool
	 * @param inheritInheritableThreadLocals when true the VirtualThreads will inherit ThreadLocal from the carrier thread
	 * @throws ThreadFactoryException Thrown if the factory can not be created. The exception message should contain instructions on how to resolve the issue
	 */
	public AbstractVirtualThreadFactory(Executor carrierThreadPool, boolean inheritInheritableThreadLocals) throws ThreadFactoryException {
		try {
			Class<?> virtualThread = Class.forName("java.lang.VirtualThread");
			//noinspection unchecked
			this.virtualThreadConstructor = (Constructor<Thread>) virtualThread.getDeclaredConstructor(Executor.class, String.class, int.class, Runnable.class);
			this.virtualThreadConstructor.setAccessible(true);
			
		} catch (InaccessibleObjectException e) {
			if (e.getMessage().contains("opens java.lang")) {
				throw new ThreadFactoryException("The Module java.base is inaccessible. To resolve this add \"--add-opens=java.base/java.lang=io.machinic.stream\" or \"--add-opens=java.base/java.lang=ALL-UNNAMED\" as a JVM argument", e);
			}
			throw new ThreadFactoryException(String.format("Accessing java.lang.VirtualThread failed. Caused by %s", e.getMessage()), e);
		} catch (Exception e) {
			throw new ThreadFactoryException(String.format("Accessing java.lang.VirtualThread failed. Caused by %s", e.getMessage()), e);
		}
		Objects.requireNonNull(carrierThreadPool);
		this.carrierThreadPool = carrierThreadPool;
		if (inheritInheritableThreadLocals) {
			this.characteristics = INHERIT_THREAD_LOCALS;
		} else {
			this.characteristics = NO_INHERIT_THREAD_LOCALS;
		}
	}
	
	/**
	 * Create a new VirtualThread with the given name and runnable.
	 * @param name name of the new {@code VirtualThread}
	 * @param task the {@code Runnable} that will be run by the new {@code VirtualThread}
	 * @return new {@code VirtualThread}
	 * @throws ThreadFactoryException Exception thrown if a new {@code VirtualThread} can't be created.
	 */
	protected Thread createVirtualThread(String name, Runnable task) throws ThreadFactoryException {
		try {
			return this.virtualThreadConstructor.newInstance(carrierThreadPool, name, characteristics, task);
		} catch (Exception e) {
			throw new ThreadFactoryException(String.format("Create new instance of java.lang.VirtualThread failed. Caused by %s", e.getMessage()), e);
		}
	}
	
}