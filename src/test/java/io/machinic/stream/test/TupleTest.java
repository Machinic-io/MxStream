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

import io.machinic.stream.Tuple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Execution(ExecutionMode.CONCURRENT)
public class TupleTest {
	
	private static final Logger LOG = LoggerFactory.getLogger(TupleTest.class);
	
	@BeforeEach
	void setUp(TestInfo testInfo) {
		LOG.info("test started: {}", testInfo.getDisplayName());
	}
	
	@Test
	public void unitTest() {
		Tuple.Unit<String> unit = Tuple.of("first");
		Assertions.assertEquals("first", unit.getFirst());
		Assertions.assertEquals("first", unit.getLeft());
		
		Tuple.Pair<String, String> pair = unit.append("second");
		Assertions.assertEquals("first", pair.getFirst());
		Assertions.assertEquals("second", pair.getSecond());
	}
	
	@Test
	public void pairTest() {
		Tuple.Pair<String, String> pair = Tuple.of("first", "second");
		Assertions.assertEquals("first", pair.getFirst());
		Assertions.assertEquals("first", pair.getLeft());
		Assertions.assertEquals("second", pair.getRight());
		Assertions.assertEquals("second", pair.getSecond());
		
		Tuple.Triplet<String, String, String> triplet = pair.append("third");
		Assertions.assertEquals("first", triplet.getFirst());
		Assertions.assertEquals("second", triplet.getSecond());
		Assertions.assertEquals("third", triplet.getThird());
	}
	
	@Test
	public void tripletTest() {
		Tuple.Triplet<String, String, String> triplet = Tuple.of("first", "second", "third");
		Assertions.assertEquals("first", triplet.getFirst());
		Assertions.assertEquals("first", triplet.getLeft());
		Assertions.assertEquals("second", triplet.getSecond());
		Assertions.assertEquals("second", triplet.getCenter());
		Assertions.assertEquals("third", triplet.getThird());
		Assertions.assertEquals("third", triplet.getRight());
		
		Tuple.Quad<String, String, String, String> quad = triplet.append("fourth");
		Assertions.assertEquals("first", quad.getFirst());
		Assertions.assertEquals("second", quad.getSecond());
		Assertions.assertEquals("third", quad.getThird());
		Assertions.assertEquals("fourth", quad.getFourth());
	}
	
	@Test
	public void quadTest() {
		Tuple.Quad<String, String, String, String> quad = Tuple.of("first", "second", "third", "fourth");
		Assertions.assertEquals("first", quad.getFirst());
		Assertions.assertEquals("first", quad.getLeft());
		Assertions.assertEquals("second", quad.getSecond());
		Assertions.assertEquals("third", quad.getThird());
		Assertions.assertEquals("fourth", quad.getFourth());
		Assertions.assertEquals("fourth", quad.getRight());
	}
	
}
