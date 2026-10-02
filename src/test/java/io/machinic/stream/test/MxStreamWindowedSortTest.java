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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static io.machinic.stream.test.TestData.INTEGER_LIST_A;
import static io.machinic.stream.test.TestData.INTEGER_SET_A;

@Execution(ExecutionMode.SAME_THREAD)
public class MxStreamWindowedSortTest {
	
	private static final Logger LOG = LoggerFactory.getLogger(MxStreamWindowedSortTest.class);
	
	@BeforeEach
	void setUp(TestInfo testInfo) {
		LOG.info("test started: {}", testInfo.getDisplayName());
	}
	
	@Test
	public void sortTest() {
		List<Integer> testData = new ArrayList<>(INTEGER_LIST_A);
		Collections.shuffle(testData);
		
		Assertions.assertEquals(INTEGER_LIST_A,
				MxStream.of(testData)
						.sorted(50, Comparator.naturalOrder())
						.toList());
	}
	
	@Test
	public void sortSmallWindowTest() {
		List<Integer> testData = new ArrayList<>(INTEGER_LIST_A);
		testData = testData.reversed();  // reverse is the worst case
		
		Assertions.assertEquals(List.of(8, 7, 6, 5, 4, 3, 2, 1, 9, 10),
				MxStream.of(testData)
						.sorted(3, Comparator.naturalOrder())
						.toList());
	}
	
	@Test
	public void sortSmallFlatMap() {
		List<Integer> testData = new ArrayList<>(INTEGER_LIST_A);
		testData = testData.reversed();  // reverse is the worst case
		
		List<Integer> finalTestData = testData;
		Assertions.assertEquals(List.of(8, 7, 6, 5, 4, 3, 2, 1, 9, 10),
				MxStream.of(List.of("thing"))
						.flatMap(record -> finalTestData.stream())
						.sorted(3, Comparator.naturalOrder())
						.toList());
	}
	
	@Test
	public void sortParallelTest() {
		List<Integer> testData = new ArrayList<>(INTEGER_LIST_A);
		Collections.shuffle(testData);
		
		Assertions.assertEquals(INTEGER_SET_A,
				MxStream.of(testData)
						.fanOut(2, 2)
						.sorted(50, Comparator.naturalOrder())
						.toSet());
	}
}
