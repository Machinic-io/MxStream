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
import java.util.List;
import java.util.stream.Collectors;

import static io.machinic.stream.test.TestData.INTEGER_LIST_A;
import static io.machinic.stream.test.TestData.INTEGER_SET_A;

@Execution(ExecutionMode.SAME_THREAD)
public class MxStreamSinkTest {
	
	private static final Logger LOG = LoggerFactory.getLogger(MxStreamSinkTest.class);
	
	@BeforeEach
	void setUp(TestInfo testInfo) {
		LOG.info("test started: {}", testInfo.getDisplayName());
	}
	
	@Test
	public void toListTest() {
		Assertions.assertEquals(INTEGER_LIST_A, MxStream.of(INTEGER_LIST_A).toList());
	}
	
	@Test
	public void toListParallelTest() {
		Assertions.assertEquals(INTEGER_LIST_A,
				MxStream.of(INTEGER_LIST_A)
						.fanOut(2, 2)
						.toList()
						.stream()
						.sorted()
						.toList());
	}
	
	@Test
	public void toSetTest() {
		Assertions.assertEquals(INTEGER_SET_A, MxStream.of(INTEGER_LIST_A.stream()).toSet());
	}
	
	@Test
	public void countTest() {
		Assertions.assertEquals(INTEGER_SET_A.size(), MxStream.of(INTEGER_LIST_A.stream()).count());
	}
	
	@Test
	public void countFanOutTest() {
		Assertions.assertEquals(INTEGER_SET_A.size(), MxStream.of(INTEGER_LIST_A).fanOut(5, 1).count());
	}
	
	@Test
	public void toStreamTest() {
		Assertions.assertEquals(INTEGER_LIST_A, MxStream.of(INTEGER_LIST_A).toStream().toList());
	}
	
	@Test
	public void toSetParallelTest() {
		Assertions.assertEquals(INTEGER_SET_A, MxStream.of(INTEGER_LIST_A.parallelStream()).toSet());
	}
	
	@Test
	public void forEachTest() {
		List<Integer> eachList = new ArrayList<>();
		MxStream.of(INTEGER_LIST_A)
				.forEach(eachList::add);
		Assertions.assertEquals(INTEGER_LIST_A, eachList);
	}
	
	@Test
	public void forEachParallelTest() {
		List<Integer> eachList = Collections.synchronizedList(new ArrayList<>());
		MxStream.of(INTEGER_LIST_A)
				.fanOut(10, 2)
				.forEach(eachList::add);
		Assertions.assertEquals(INTEGER_LIST_A, eachList.stream().sorted().toList());
	}
	
	@Test
	public void collectorCountingTest() {
		Assertions.assertEquals(INTEGER_LIST_A.size(), MxStream.of(INTEGER_LIST_A)
				.collect(Collectors.counting()));
	}
	
	@Test
	public void collectorCountingParallelTest() {
		Assertions.assertEquals(INTEGER_LIST_A.size(),
				MxStream.of(INTEGER_LIST_A)
						.fanOut(4, 2)
						.collect(Collectors.counting()));
	}
	
}
