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

package io.machinic.stream.test;

import io.machinic.stream.concurrent.PriorityThreadFactory;
import io.machinic.stream.concurrent.VirtualThreadFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

@Execution(ExecutionMode.SAME_THREAD)
public class VirtualThreadFactoryTest {
	private static final Logger LOG = LoggerFactory.getLogger(VirtualThreadFactoryTest.class);
	
	@BeforeEach
	void setUp(TestInfo testInfo) {
		LOG.info("test started: {}", testInfo.getDisplayName());
	}
	
	@Test
	void testVirtualThreadPerTaskExecutor() throws Exception {
		// Create platform thread pool that will be used to execute Virtual Threads.
		try (ExecutorService carrierTheadPool = Executors.newFixedThreadPool(2, new PriorityThreadFactory("CarrierThread", Thread.MIN_PRIORITY))) {
			// Create a new VirtualThreadExecutor using PooledVirtualThreadFactory so the carrier threads are assigned by the carrierThreadPool rather than the global pool
			try (ExecutorService virtualThreadExecutor = VirtualThreadFactory.builder().carrierThreadPool(carrierTheadPool).namePrefix("ExampleThread").build().newThreadPerTaskExecutor()) {
				
				int taskCount = 1000;
				AtomicInteger finishCounter = new AtomicInteger();
				// Create a bunch of blocking tasks
				List<Future<?>> tasks = new ArrayList<>();
				for (int i = 0; i < taskCount; i++) {
					tasks.add(virtualThreadExecutor.submit(() -> {
						Assertions.assertTrue(Thread.currentThread().isVirtual());
						Assertions.assertTrue(Thread.currentThread().getName().startsWith("ExampleThread"));
						Assertions.assertTrue(Thread.currentThread().toString().contains("CarrierThread"));
						
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							throw new RuntimeException(e);
						}
						finishCounter.incrementAndGet();
					}));
				}
				// Block until everything is done
				for (Future<?> task : tasks) {
					task.get();
				}
				Assertions.assertEquals(taskCount, finishCounter.get(), "Some tasks didn't finish");
			}
		}
	}
	
	@Test
	void testFixedVirtualThreadExecutor() throws Exception {
		// Create platform thread pool that will be used to execute Virtual Threads.
		try (ExecutorService carrierTheadPool = Executors.newFixedThreadPool(2, new PriorityThreadFactory("CarrierThread", Thread.MIN_PRIORITY))) {
			// Create a new VirtualThreadExecutor using PooledVirtualThreadFactory so the carrier threads are assigned by the carrierThreadPool rather than the global pool
			try (ExecutorService virtualThreadExecutor = VirtualThreadFactory.builder().carrierThreadPool(carrierTheadPool).namePrefix("ExampleThread").counter(1).build().newFixedThreadPool(3)) {
				
				int taskCount = 1000;
				AtomicInteger finishCounter = new AtomicInteger();
				// Create a bunch of blocking tasks
				List<Future<?>> tasks = new ArrayList<>();
				for (int i = 0; i < taskCount; i++) {
					tasks.add(virtualThreadExecutor.submit(() -> {
						Assertions.assertTrue(Thread.currentThread().isVirtual());
						Assertions.assertTrue(Thread.currentThread().getName().startsWith("ExampleThread"));
						Assertions.assertTrue(Thread.currentThread().toString().contains("CarrierThread"));
						try {
							Thread.sleep(2);
						} catch (InterruptedException e) {
							throw new RuntimeException(e);
						}
						finishCounter.incrementAndGet();
					}));
				}
				// Block until everything is done
				for (Future<?> task : tasks) {
					task.get();
				}
				Assertions.assertEquals(taskCount, finishCounter.get(), "Some tasks didn't finish");
			}
		}
	}
	
}