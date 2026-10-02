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
import io.machinic.stream.test.utils.IntegerGeneratorIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.machinic.stream.test.TestData.INTEGER_LIST_A;
import static io.machinic.stream.test.TestData.INTEGER_SET_A;
import static io.machinic.stream.test.TestData.INTEGER_SET_B;
import static io.machinic.stream.test.TestData.NOOP_EXCEPTION_HANDLER;

@Execution(ExecutionMode.CONCURRENT)
public class MxStreamFanOutTest {
	
	private static final Logger LOG = LoggerFactory.getLogger(MxStreamFanOutTest.class);
	
	@BeforeEach
	void setUp(TestInfo testInfo) {
		LOG.info("test started: {}", testInfo.getDisplayName());
	}
	
	@Test
	public void fanOutEmptyInputTest() {
		Assertions.assertEquals(List.of(),
				MxStream.of(List.of())
						.fanOut(2, 100)
						.toList());
	}
	
	@Test
	public void fanOutSingleElementInputTest() {
		Assertions.assertEquals(List.of(0),
				MxStream.of(List.of(0))
						.fanOut(2, 100)
						.toList());
	}
	
	@Test
	public void fanOutSmallBufferTest() {
		Assertions.assertEquals(INTEGER_SET_A, MxStream.of(INTEGER_LIST_A)
				.fanOut(2, 1)
				.toSet());
	}
	
	@Test
	public void fanOutLargeBufferTest() {
		Assertions.assertEquals(INTEGER_SET_A, MxStream.of(INTEGER_LIST_A)
				.fanOut(2, 50)
				.toSet());
	}
	
	@Test
	public void fanOutParallelTest() {
		Assertions.assertEquals(INTEGER_SET_A, MxStream.of(INTEGER_LIST_A)
				.fanOut(2, 50)
				.toSet());
	}
	
	@Test
	public void fanOutWithSlowDownstreamTest() {
		Assertions.assertEquals(INTEGER_SET_A, MxStream.of(INTEGER_LIST_A)
				.fanOut(10, 50)
				.peek(value -> {
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				})
				.toSet());
	}
	
	@Test
	public void fanOutWithAsyncMapTest() {
		
		Assertions.assertEquals(
				100L, MxStream.of(new IntegerGeneratorIterator(100).toStream()).fanOut(1, 50).asyncMap(10, value -> {
							try {
								Thread.sleep(10);
							} catch (InterruptedException e) {
								throw new RuntimeException(e);
							}
							return Integer.toString(value);
						})
						.batch(10, 500, TimeUnit.MILLISECONDS)
						.asyncMap(10, batch -> {
							try {
								Thread.sleep(10);
							} catch (InterruptedException e) {
								throw new RuntimeException(e);
							}
							return batch;
						})
						.flatMap(batch -> batch.stream())
						.count());
	}
	
	@Test
	public void fanOutPeekDefaultExceptionHandler() {
		Exception exception = Assertions.assertThrows(StreamException.class, () -> {
			MxStream.of(INTEGER_LIST_A)
					.fanOut(2, 5)
					.peek(value -> {
						throw new RuntimeException("peek operation exception");
					}).toList();
		});
		Assertions.assertEquals("Stream failed with unhandled exception: peek operation exception", exception.getMessage());
	}
	
	@Test
	public void fanOutPeekInterruptException() {
		Exception exception = Assertions.assertThrows(StreamException.class, () -> {
			MxStream.of(INTEGER_LIST_A)
					.fanOut(2, 5)
					.peek(value -> {
						Thread.currentThread().interrupt();
					}).toList();
		});
		Assertions.assertEquals("FanOutSpliterator was interrupted", exception.getMessage());
	}
	
	@Test
	public void fanOutPeekParallelCustomExceptionHandler() {
		Assertions.assertEquals(INTEGER_SET_B,
				MxStream.of(INTEGER_SET_B)
						.exceptionHandler(NOOP_EXCEPTION_HANDLER)
						.fanOut(2, 5)
						.peek(integer -> {
							if (integer % 2 != 0) {
								throw new RuntimeException("peek operation exception");
							}
						}).toSet());
	}
}
