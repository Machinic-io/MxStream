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
import static io.machinic.stream.test.TestData.INTEGER_LIST_D;
import static io.machinic.stream.test.TestData.INTEGER_LIST_E;

@Execution(ExecutionMode.SAME_THREAD)
public class MxStreamBatchTest {
	private static final Logger LOG = LoggerFactory.getLogger(MxStreamBatchTest.class);
	
	@BeforeEach
	void setUp(TestInfo testInfo) {
		LOG.info("test started: {}", testInfo.getDisplayName());
	}
	
	@Test
	public void batchTest() {
		Assertions.assertEquals(INTEGER_LIST_D,
				MxStream.of(INTEGER_LIST_A)
						.batch(5)
						.toList());
	}
	
	@Test
	public void batchTestWithFlatMap() {
		List<List<Integer>> batches = MxStream.of(
						MxStream.of(new IntegerGeneratorIterator(100))
								.batch(50)
								.toList())
				.flatMap(List::stream)
				.batch(5)
				.toList();
		
		Assertions.assertEquals(20, batches.size());
	}
	
	@Test
	public void batchTimeoutTestWithFlatMap() {
		List<List<Integer>> batches = MxStream.of(
						MxStream.of(new IntegerGeneratorIterator(100))
								.batch(50)
								.toList())
				.flatMap(List::stream)
				.batch(5, 300, TimeUnit.SECONDS)
				.toList();
		Assertions.assertEquals(20, batches.size());
	}
	
	@Test
	public void batchNoItemsTest() {
		Assertions.assertEquals(List.of(),
				MxStream.of(List.of())
						.batch(5)
						.toList());
	}
	
	@Test
	public void batchBatchSizeExceptionTest() {
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> MxStream.of(INTEGER_LIST_A)
						.batch(0)
						.toList());
	}
	
	@Test
	public void batchWithOutTimeoutTest() {
		Assertions.assertEquals(INTEGER_LIST_D,
				MxStream.of(INTEGER_LIST_A)
						.batch(5, 5, TimeUnit.SECONDS)
						.toList());
	}
	
	@Test
	public void batchWithTimeNoItemsTest() {
		Assertions.assertEquals(List.of(),
				MxStream.of(List.of())
						.batch(5, 5, TimeUnit.SECONDS)
						.toList());
	}
	
	@Test
	public void batchTimeoutExceptionTest() {
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> MxStream.of(INTEGER_LIST_A)
						.batch(5, 0, TimeUnit.SECONDS)
						.toList());
	}
	
	@Test
	public void batchWithTimeoutTest() {
		Assertions.assertEquals(INTEGER_LIST_E,
				MxStream.of(INTEGER_LIST_A)
						.peek(value -> {
							try {
								Thread.sleep(5);
							} catch (InterruptedException e) {
								throw new RuntimeException(e);
							}
						})
						.batch(4, 1, TimeUnit.MILLISECONDS)
						.toList());
	}
	
}
