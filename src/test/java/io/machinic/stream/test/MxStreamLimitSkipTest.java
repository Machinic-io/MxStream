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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

@Execution(ExecutionMode.CONCURRENT)
public class MxStreamLimitSkipTest {
	private static final Logger LOG = LoggerFactory.getLogger(MxStreamLimitSkipTest.class);
	
	@BeforeEach
	void setUp(TestInfo testInfo) {
		LOG.info("test started: {}", testInfo.getDisplayName());
	}
	
	@Test
	public void testSkip() {
		List<Integer> result = MxStream.of(Arrays.asList(1, 2, 3, 4, 5))
				.skip(2)
				.toList();
		Assertions.assertEquals(List.of(3, 4, 5), result, "Skipping first two elements should result in [3, 4, 5]");
	}
	
	@Test
	public void testSkipFanOutOne() {
		List<Integer> result = MxStream.of(Arrays.asList(1, 2, 3, 4, 5))
				.fanOut(1,5)
				.skip(2)
				.toList();
		Assertions.assertEquals(List.of(3, 4, 5), result, "Skipping first two elements should result in [3, 4, 5]");
	}
	
	@Test
	public void testSkipFanOut() {
		Set<Integer> result = MxStream.of(Arrays.asList(1, 2, 3, 4, 5))
				.fanOut(2,5)
				.skip(2)
				.toSet();
		Assertions.assertEquals(3, result.size(), "With more than one thread the results are non-deterministic");
	}
	
	@Test
	public void testSkipZero() {
		List<Integer> result = MxStream.of(Arrays.asList(1, 2, 3, 4, 5))
				.skip(0)
				.toList();
		Assertions.assertEquals(Arrays.asList(1, 2, 3, 4, 5), result, "Nothing should be skipped");
	}
	
	@Test
	public void testSkipIllegalValue() {
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> MxStream.of(Arrays.asList(1, 2, 3, 4, 5))
						.skip(-1)
						.toList());
	}
	
	@Test
	public void testLimit() {
		List<Integer> result = MxStream.of(Arrays.asList(1, 2, 3, 4, 5))
				.limit(2)
				.peek(value -> Assertions.assertTrue(value <= 2))
				.toList();
		Assertions.assertEquals(List.of(1, 2), result, "Limiting to two elements should result in [1, 2]");
	}
	
	@Test
	public void testLimitFanOutOne() {
		List<Integer> result = MxStream.of(Arrays.asList(1, 2, 3, 4, 5))
				.fanOut(1,5)
				.limit(2)
				.peek(value -> Assertions.assertTrue(value <= 2))
				.toList();
		Assertions.assertEquals(List.of(1, 2), result, "Limiting to two elements should result in [1, 2]");
	}
	
	@Test
	public void testLimitFanOut() {
		List<Integer> result = MxStream.of(Arrays.asList(1, 2, 3, 4, 5))
				.fanOut(2,5)
				.limit(2)
				.peek(value -> Assertions.assertTrue(value <= 2))
				.toList();
		Assertions.assertEquals(2, result.size(), "Limiting on a parallel stream can be non-deterministic");
	}
	
	@Test
	public void testLimitIllegalValue() {
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> MxStream.of(Arrays.asList(1, 2, 3, 4, 5))
						.limit(0)
						.toList());
	}
	
}
