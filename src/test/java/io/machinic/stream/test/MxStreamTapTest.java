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

import io.machinic.stream.MxStream;
import io.machinic.stream.StreamException;
import io.machinic.stream.TapBuilder;
import io.machinic.stream.test.utils.IntegerGeneratorIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Execution(ExecutionMode.SAME_THREAD)
public class MxStreamTapTest {
	private static final Logger LOG = LoggerFactory.getLogger(MxStreamTapTest.class);
	
	@BeforeEach
	void setUp(TestInfo testInfo) {
		LOG.info("test started: {}", testInfo.getDisplayName());
	}
	
	@Test
	public void streamTapTest() throws InterruptedException {
		TapBuilder<String> tapBuilder = new TapBuilder<>(5);
		
		AtomicLong counter = new AtomicLong(0);
		Thread streamThread = new Thread(() -> {
			MxStream.of(new IntegerGeneratorIterator(500))
					.map(integer -> Integer.toString(integer))
					.tap(tapBuilder)
					.forEach(value -> counter.incrementAndGet());
		});
		streamThread.setName("stream-thread");
		streamThread.start();
		
		long tapCount = tapBuilder.awaitBuild()
				.collect(Collectors.counting());
		
		Assertions.assertEquals(500, counter.get());
		Assertions.assertEquals(500, tapCount);
	}
	
	@Test
	public void streamTapParallelTest() throws InterruptedException {
		TapBuilder<String> tapBuilder = new TapBuilder<String>(5)
				.parallel(true)
				.parallelism(2);
		
		AtomicLong counter = new AtomicLong(0);
		Thread streamThread = new Thread(() -> {
			MxStream.of(new IntegerGeneratorIterator(500))
					.fanOut(2, 2)
					.map(integer -> Integer.toString(integer))
					.tap(tapBuilder)
					.forEach(value -> counter.incrementAndGet());
		});
		streamThread.setName("stream-thread");
		streamThread.start();
		
		long tapCount = tapBuilder.awaitBuild()
				.collect(Collectors.counting());
		
		Assertions.assertEquals(500, counter.get());
		Assertions.assertEquals(500, tapCount);
	}
	
	@Test
	public void streamExceptionTest() throws InterruptedException {
		TapBuilder<String> tapBuilder = new TapBuilder<>(5);
		
		Thread streamThread = new Thread(() -> {
			Assertions.assertThrows(StreamException.class, () -> {
				MxStream.of(new IntegerGeneratorIterator(500))
						.map(integer -> Integer.toString(integer))
						.tap(tapBuilder)
						.forEach(value -> {
							if ("400".equals(value)) {
								throw new RuntimeException("Test exception");
							}
						});
			});
		});
		streamThread.setName("stream-thread");
		streamThread.start();
		
		Assertions.assertThrows(StreamException.class, () -> {
			tapBuilder.awaitBuild()
					.collect(Collectors.counting());
		});
	}
	
	@Test
	public void streamTapExceptionTest() throws InterruptedException {
		TapBuilder<String> tapBuilder = new TapBuilder<>(5);
		
		Thread streamThread = new Thread(() -> {
			Assertions.assertThrows(StreamException.class, () -> {
				tapBuilder.awaitBuild()
						.collect(Collectors.counting());
			});
		});
		streamThread.setName("tap-stream-thread");
		streamThread.start();
		
		Assertions.assertThrows(StreamException.class, () -> {
			MxStream.of(new IntegerGeneratorIterator(500))
					.map(integer -> Integer.toString(integer))
					.tap(tapBuilder)
					.forEach(value -> {
						if ("400".equals(value)) {
							throw new RuntimeException("Test exception");
						}
					});
		});
		
	}
	
}
