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
import io.machinic.stream.test.utils.CountingSupplier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.machinic.stream.test.TestData.INTEGER_LIST_A;
import static io.machinic.stream.test.TestData.INTEGER_SET_A;
import static io.machinic.stream.test.TestData.INTEGER_SET_B;
import static io.machinic.stream.test.TestData.NOOP_EXCEPTION_HANDLER;

@Execution(ExecutionMode.SAME_THREAD)
public class MxStreamPeekTest {
	
	private static final Logger LOG = LoggerFactory.getLogger(MxStreamPeekTest.class);
	
	@BeforeEach
	void setUp(TestInfo testInfo) {
		LOG.info("test started: {}", testInfo.getDisplayName());
	}
	
	@Test
	public void peekTest() {
		AtomicInteger index = new AtomicInteger(0);
		Assertions.assertEquals(INTEGER_LIST_A, MxStream.of(INTEGER_LIST_A).peek(integer -> {
			Assertions.assertEquals(INTEGER_LIST_A.get(index.getAndIncrement()), integer);
		}).toList());
	}
	
	@Test
	public void peekParallelTest() {
		AtomicInteger counter = new AtomicInteger(0);
		Assertions.assertEquals(INTEGER_SET_A,
				MxStream.of(INTEGER_LIST_A)
						.fanOut(2, 2)
						.peek(integer -> {
							counter.getAndIncrement();
						}).toSet());
		Assertions.assertEquals(INTEGER_LIST_A.size(), counter.get());
	}
	
	@Test
	public void peekSupplierTest() {
		CountingSupplier<Consumer<? super Integer>> supplier = new CountingSupplier<>(integer -> {
		});
		Assertions.assertEquals(INTEGER_LIST_A, MxStream.of(INTEGER_LIST_A).peek(supplier).toList());
		// Supplier should only be called once on a sequential stream
		Assertions.assertEquals(1, supplier.getCount());
	}
	
	@Test
	public void peekParallelSupplierTest() {
		CountingSupplier<Consumer<? super Integer>> supplier = new CountingSupplier<>(integer -> {
		});
		Assertions.assertEquals(INTEGER_SET_A,
				MxStream.of(INTEGER_LIST_A)
						.fanOut(3, 2)
						.peek(supplier)
						.toSet());
		// Supplier should be called once by the main thread and once for each additional thread
		Assertions.assertEquals(4, supplier.getCount());
	}
	
	@Test
	public void peekDefaultExceptionHandler() {
		Exception exception = Assertions.assertThrows(StreamException.class, () -> {
			MxStream.of(INTEGER_LIST_A).peek(value -> {
				throw new RuntimeException("peek operation exception");
			}).toList();
		});
		Assertions.assertEquals("Stream failed with unhandled exception: peek operation exception", exception.getMessage());
	}
	
	@Test
	public void peekParallelDefaultExceptionHandler() {
		Exception exception = Assertions.assertThrows(StreamException.class, () -> {
			MxStream.of(INTEGER_LIST_A)
					.fanOut(2, 2)
					.peek(value -> {
						throw new RuntimeException("peek operation exception");
					}).toList();
		});
		Assertions.assertEquals("Stream failed with unhandled exception: peek operation exception", exception.getMessage());
	}
	
	@Test
	public void peekCustomExceptionHandler() {
		Assertions.assertArrayEquals(INTEGER_LIST_A.toArray(), MxStream.of(INTEGER_LIST_A).exceptionHandler(NOOP_EXCEPTION_HANDLER).peek(integer -> {
			if (integer % 2 != 0) {
				throw new RuntimeException("peek operation exception");
			}
		}).toList().toArray());
	}
	
	@Test
	public void peekParallelCustomExceptionHandler() {
		Assertions.assertEquals(INTEGER_SET_B,
				MxStream.of(INTEGER_SET_B)
						.fanOut(4, 2)
						.exceptionHandler(NOOP_EXCEPTION_HANDLER)
						.peek(integer -> {
							if (integer % 2 != 0) {
								throw new RuntimeException("peek operation exception");
							}
						}).toSet());
	}
}
