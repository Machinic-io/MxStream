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
import io.machinic.stream.StreamEventException;
import io.machinic.stream.StreamException;
import io.machinic.stream.StreamInterruptedException;
import io.machinic.stream.test.utils.CountingSupplier;
import io.machinic.stream.test.utils.IntegerGeneratorIterator;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.machinic.stream.test.TestData.INTEGER_LIST_A;
import static io.machinic.stream.test.TestData.NOOP_EXCEPTION_HANDLER;
import static io.machinic.stream.test.TestData.STRING_LIST_A;
import static io.machinic.stream.test.TestData.STRING_SET_A;
import static io.machinic.stream.test.TestData.STRING_SET_B;

@Execution(ExecutionMode.CONCURRENT)
public class MxStreamAsyncMapTest {
	private static final Logger LOG = LoggerFactory.getLogger(MxStreamAsyncMapTest.class);
	
	@BeforeEach
	void setUp(TestInfo testInfo) {
		LOG.info("test started: {}", testInfo.getDisplayName());
	}
	
	@Test
	public void asyncMapTest() {
		Assertions.assertEquals(STRING_LIST_A, MxStream.of(INTEGER_LIST_A).asyncMap(1, integer -> Integer.toString(integer)).toList());
	}
	
	@Test
	public void asyncMapEmptyTest() {
		MxStream.of(List.of())
				.asyncMap(5, value -> value)
				.toList();
	}
	
	@Test
	public void asyncMapNullsTest() {
		List<Integer> list = new ArrayList<>();
		list.add(1);
		list.add(2);
		list.add(null);
		list.add(3);
		
		Assertions.assertEquals(list,
				MxStream.of(list)
						.asyncMap(1, value -> value)
						.toList());
	}
	
	@Test
	public void asyncMapLargerParallelismTest() {
		Assertions.assertEquals(STRING_LIST_A, MxStream.of(INTEGER_LIST_A).asyncMap(100, integer -> Integer.toString(integer)).toList());
	}
	
	@Test
	public void asyncMapWithFlatMap() {
		List<Integer> integers = MxStream.of(
						MxStream.of(new IntegerGeneratorIterator(100))
								.batch(50)
								.toList())
				.flatMap(List::stream)
				.asyncMap(2, value -> {
					return value;
				})
				.toList();
		Assertions.assertEquals(100, integers.size());
	}
	
	@Test
	public void asyncMapParallelTest() {
		Assertions.assertEquals(STRING_SET_A,
				MxStream.of(INTEGER_LIST_A)
						.fanOut(2, 2)
						.asyncMap(1, integer -> Integer.toString(integer))
						.toSet());
	}
	
	@Test
	public void asyncMapParallelLargerParallelismTest() {
		Assertions.assertEquals(STRING_SET_A,
				MxStream.of(INTEGER_LIST_A)
						.fanOut(2, 2)
						.asyncMap(10, integer -> Integer.toString(integer))
						.toSet());
	}
	
	@Test
	public void mapSupplierTest() {
		CountingSupplier<Function<? super Integer, ? extends String>> supplier =
				new CountingSupplier<>(integer ->
						Integer.toString(integer)
				);
		Assertions.assertEquals(STRING_LIST_A, MxStream.of(INTEGER_LIST_A).asyncMap(2, supplier).toList());
		// Supplier should only be called once on a sequential stream
		Assertions.assertEquals(1, supplier.getCount());
	}
	
	@Test
	public void mapParallelSupplierTest() {
		CountingSupplier<Function<? super Integer, ? extends String>> supplier = new CountingSupplier<>(integer -> Integer.toString(integer));
		Assertions.assertEquals(STRING_SET_A,
				MxStream.of(INTEGER_LIST_A)
						.fanOut(3, 2)
						.asyncMap(2, supplier)
						.toSet());
		// Supplier should be called once by the main thread and once for each additional thread
		Assertions.assertEquals(4, supplier.getCount());
	}
	
	@Test
	public void mapDefaultExceptionHandler() {
		Exception exception = Assertions.assertThrows(StreamException.class, () -> {
			MxStream.of(INTEGER_LIST_A).asyncMap(2, value -> {
				throw new RuntimeException("map operation exception");
			}).toList();
		});
		Assertions.assertEquals("Stream failed with unhandled exception: map operation exception", exception.getMessage());
	}
	
	@Test
	public void mapParallelDefaultExceptionHandler() {
		Exception exception = Assertions.assertThrows(StreamException.class, () -> {
			MxStream.of(INTEGER_LIST_A)
					.fanOut(2, 2)
					.asyncMap(2, value -> {
						throw new RuntimeException("map operation exception");
					}).toList();
		});
		Assertions.assertEquals("Stream failed with unhandled exception: map operation exception", exception.getMessage());
	}
	
	@Test
	public void mapCustomExceptionHandler() {
		Assertions.assertEquals(STRING_SET_B,
				MxStream.of(INTEGER_LIST_A)
						.exceptionHandler(NOOP_EXCEPTION_HANDLER)
						.asyncMap(2, integer -> {
							if (integer % 2 != 0) {
								throw new RuntimeException("map operation exception");
							}
							return Integer.toString(integer);
						}).toSet());
	}
	
	@Test
	public void mapParallelCustomExceptionHandler() {
		Assertions.assertEquals(STRING_SET_B,
				MxStream.of(INTEGER_LIST_A)
						.fanOut(4, 2)
						.exceptionHandler(NOOP_EXCEPTION_HANDLER)
						.asyncMap(2, integer -> {
							if (integer % 2 != 0) {
								throw new RuntimeException("map operation exception");
							}
							return Integer.toString(integer);
						}).toSet());
	}
	
	@Test
	public void asyncMapStallTest() {
		StreamException exception = Assertions.assertThrows(StreamException.class, () -> {
			MxStream.of(INTEGER_LIST_A)
					.asyncTimeoutMillis(100)
					.asyncMap(2, integer -> {
						if (integer % 2 != 0) {
							// simulate stalled task
							try {
								Thread.sleep(99999);
							} catch (InterruptedException e) {
								throw new StreamInterruptedException(e);
							}
						}
						return Integer.toString(integer);
					}).toSet();
		});
		Assertions.assertEquals("asyncMap has been interrupted", exception.getMessage());
	}
	
	@Test
	public void asyncMapStallCustomTimeoutTest() {
		StreamException exception = Assertions.assertThrows(StreamException.class, () -> {
			MxStream.of(INTEGER_LIST_A)
					.asyncMap(2, 100L, integer -> {
						if (integer % 2 != 0) {
							// simulate stalled task
							try {
								Thread.sleep(99999);
							} catch (InterruptedException e) {
								throw new StreamInterruptedException(e);
							}
						}
						return Integer.toString(integer);
					}).toSet();
		});
		Assertions.assertEquals("asyncMap has been interrupted", exception.getMessage());
	}
	
	@Test
	public void asyncMapStallNoopErrorHandlerTest() {
		Assertions.assertEquals(STRING_SET_B,
				MxStream.of(INTEGER_LIST_A)
						.asyncTimeoutMillis(100)
						.exceptionHandler(NOOP_EXCEPTION_HANDLER)
						.asyncMap(2, integer -> {
							if (integer % 2 != 0) {
								// simulate stalled task
								try {
									Thread.sleep(99999);
								} catch (InterruptedException e) {
									throw new StreamInterruptedException(e);
								}
							}
							return Integer.toString(integer);
						}).toSet());
	}
	
	@Test
	public void asyncMapTaskRejected() {
		try (ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1))) {
			StreamException exception = Assertions.assertThrows(StreamException.class, () -> {
				MxStream.of(INTEGER_LIST_A)
						.exceptionHandler(NOOP_EXCEPTION_HANDLER)
						.asyncMap(999, executor, integer -> {
							try {
								// simulate stalled task
								Thread.sleep(100);
							} catch (InterruptedException e) {
								throw new RuntimeException(e);
							}
							return Integer.toString(integer);
						}).toSet();
			});
			Assertions.assertEquals("Failed to enqueue task. Caused by RejectedExecutionException", exception.getMessage());
		}
	}
	
	@Test
	public void asyncMapStreamEventExceptionTest() {
		Assertions.assertEquals(STRING_SET_B,
				MxStream.of(INTEGER_LIST_A.stream().filter(integer -> integer % 2 == 0))
						.asyncTimeoutMillis(100)
						.asyncMap(2, integer -> {
							if (integer % 2 != 0) {
								throw new StreamEventException(integer, "Event Failed");
							}
							return Integer.toString(integer);
						}).toSet());
	}
}
