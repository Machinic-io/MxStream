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
import io.machinic.stream.util.ErrorTestIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.concurrent.ForkJoinPool;

import static io.machinic.stream.test.TestData.INTEGER_LIST_A;
import static io.machinic.stream.test.TestData.INTEGER_SET_A;
import static io.machinic.stream.test.TestData.STRING_A;
import static io.machinic.stream.test.TestData.STRING_LIST_A;

@Execution(ExecutionMode.CONCURRENT)
public class MxStreamSourceTest {
	
	private static final Logger LOG = LoggerFactory.getLogger(MxStreamSourceTest.class);
	
	@BeforeEach
	void setUp(TestInfo testInfo) {
		LOG.info("test started: {}", testInfo.getDisplayName());
	}
	
	@Test
	public void sourceListTest() {
		Assertions.assertEquals(INTEGER_LIST_A, MxStream.of(INTEGER_LIST_A).toList());
	}
	
	@Test
	public void sourceListFanOutTest() {
		Assertions.assertEquals(INTEGER_SET_A,
				MxStream.of(INTEGER_LIST_A)
						.fanOut(2, 2)
						.toSet());
	}
	
	@Test
	public void sourceListFanOutThreadThreadPoolTest() {
		Assertions.assertEquals(INTEGER_SET_A,
				MxStream.of(INTEGER_LIST_A)
						.fanOut(2, 2, ForkJoinPool.commonPool())
						.toSet());
	}
	
	@Test
	public void sourceStreamTest() {
		Assertions.assertEquals(INTEGER_LIST_A, MxStream.of(INTEGER_LIST_A.stream()).toList());
	}
	
	@Test
	public void sourceStreamParallelTest() {
		Assertions.assertEquals(INTEGER_SET_A, MxStream.of(INTEGER_LIST_A.parallelStream()).toSet());
		Assertions.assertEquals(INTEGER_SET_A, MxStream.of(INTEGER_LIST_A.parallelStream(), 3, ForkJoinPool.commonPool()).toSet());
	}
	
	@Test
	public void sourceIteratorTest() {
		Assertions.assertEquals(INTEGER_LIST_A, MxStream.of(INTEGER_LIST_A.iterator()).toList());
	}
	
	@Test
	public void sourceBufferedReaderTest() {
		BufferedReader bufferedReader = new BufferedReader(new StringReader(STRING_A));
		Assertions.assertEquals(STRING_LIST_A, MxStream.of(bufferedReader).toList());
	}
	
	@Test
	public void sourceBufferedReaderTestWithSkip() {
		BufferedReader bufferedReader = new BufferedReader(new StringReader(STRING_A));
		Assertions.assertEquals(STRING_LIST_A.stream().skip(1).toList(), MxStream.of(bufferedReader).skip(1).toList());
	}
	
	@Test
	public void sourceBufferedReaderTestWithLimit() {
		BufferedReader bufferedReader = new BufferedReader(new StringReader(STRING_A));
		Assertions.assertEquals(STRING_LIST_A.stream().limit(2).toList(), MxStream.of(bufferedReader).limit(2).toList());
	}
	
	@Test
	public void iteratorSourceExceptionTest() {
		Exception exception = Assertions.assertThrows(StreamException.class, () -> {
			MxStream.of(new ErrorTestIterator<>(INTEGER_LIST_A))
					.toList();
		});
		Assertions.assertEquals("An error occurred while processing a stream. Caused by Iterator has thrown an exception", exception.getMessage());
	}
	
	@Test
	public void sourceStreamExceptionTest() {
		Exception exception = Assertions.assertThrows(StreamException.class, () -> {
			MxStream.of(INTEGER_LIST_A.stream()
							.peek(value -> {
								if (value == 2) {
									throw new RuntimeException("Error on 2");
								}
							}))
					.toList();
		});
		Assertions.assertEquals("An error occurred while processing a stream. Caused by Error on 2", exception.getMessage());
	}
	
}
