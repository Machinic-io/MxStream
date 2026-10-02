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
import java.util.function.Predicate;

import static io.machinic.stream.test.TestData.INTEGER_LIST_A;
import static io.machinic.stream.test.TestData.INTEGER_LIST_B;
import static io.machinic.stream.test.TestData.INTEGER_SET_B;
import static io.machinic.stream.test.TestData.NOOP_EXCEPTION_HANDLER;

@Execution(ExecutionMode.SAME_THREAD)
public class MxStreamFilterTest {
	
	private static final Logger LOG = LoggerFactory.getLogger(MxStreamFilterTest.class);
	
	@BeforeEach
	void setUp(TestInfo testInfo) {
		LOG.info("test started: {}", testInfo.getDisplayName());
	}
	
	@Test
	public void filterTest() {
		Assertions.assertEquals(INTEGER_LIST_B, MxStream.of(INTEGER_LIST_A).filter(integer -> {
			return integer % 2 == 0;
		}).toList());
	}
	
	@Test
	public void filterCountTest() {
		AtomicInteger index = new AtomicInteger(0);
		Assertions.assertEquals(INTEGER_LIST_A, MxStream.of(INTEGER_LIST_A).filter(integer -> {
			Assertions.assertEquals(INTEGER_LIST_A.get(index.getAndIncrement()), integer);
			return true;
		}).toList());
	}
	
	@Test
	public void filterParallelTest() {
		Assertions.assertArrayEquals(INTEGER_LIST_B.toArray(),
				MxStream.of(INTEGER_LIST_A)
						.fanOut(2, 2)
						.filter(integer -> {
							return integer % 2 == 0;
						}).toSet().toArray());
	}
	
	@Test
	public void filterCountParallelTest() {
		AtomicInteger counter = new AtomicInteger(0);
		Assertions.assertArrayEquals(INTEGER_LIST_A.toArray(),
				MxStream.of(INTEGER_LIST_A)
						.fanOut(2, 2)
						.filter(integer -> {
							counter.getAndIncrement();
							return true;
						}).toSet().toArray());
		Assertions.assertEquals(INTEGER_LIST_A.size(), counter.get());
	}
	
	@Test
	public void filterSupplierTest() {
		CountingSupplier<Predicate<? super Integer>> supplier = new CountingSupplier<>(integer -> integer % 2 == 0);
		Assertions.assertEquals(INTEGER_LIST_B, MxStream.of(INTEGER_LIST_A).filter(supplier).toList());
		// Supplier should only be called once on a sequential stream
		Assertions.assertEquals(1, supplier.getCount());
	}
	
	@Test
	public void filterParallelSupplierTest() {
		CountingSupplier<Predicate<? super Integer>> supplier = new CountingSupplier<>(integer -> integer % 2 == 0);
		Assertions.assertEquals(INTEGER_SET_B,
				MxStream.of(INTEGER_LIST_A)
						.fanOut(3, 2)
						.filter(supplier)
						.toSet());
		// Supplier should be called once by the main thread and once for each additional thread
		Assertions.assertEquals(4, supplier.getCount());
	}
	
	@Test
	public void filterDefaultExceptionHandler() {
		Exception exception = Assertions.assertThrows(StreamException.class, () -> {
			MxStream.of(INTEGER_LIST_A)
					.filter(value -> {
						throw new RuntimeException("filter operation exception");
					}).toList();
		});
		Assertions.assertEquals("Stream failed with unhandled exception: filter operation exception", exception.getMessage());
	}
	
	@Test
	public void filterParallelDefaultExceptionHandler() {
		Exception exception = Assertions.assertThrows(StreamException.class, () -> {
			MxStream.of(INTEGER_LIST_A)
					.fanOut(2, 2)
					.filter(value -> {
						throw new RuntimeException("filter operation exception");
					}).toList();
		});
		Assertions.assertEquals("Stream failed with unhandled exception: filter operation exception", exception.getMessage());
	}
	
	@Test
	public void filterCustomExceptionHandler() {
		Assertions.assertEquals(INTEGER_SET_B, MxStream.of(INTEGER_LIST_A).exceptionHandler(NOOP_EXCEPTION_HANDLER).filter(integer -> {
			if (integer % 2 != 0) {
				throw new RuntimeException("map operation exception");
			}
			return true;
		}).toSet());
	}
	
	@Test
	public void filterParallelCustomExceptionHandler() {
		Assertions.assertEquals(INTEGER_SET_B,
				MxStream.of(INTEGER_LIST_A)
						.fanOut(4, 2)
						.exceptionHandler(NOOP_EXCEPTION_HANDLER)
						.fanOut(2, 2)
						.filter(integer -> {
							if (integer % 2 != 0) {
								throw new RuntimeException("map operation exception");
							}
							return true;
						}).toSet());
	}
	
}
