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

package io.machinic.stream.test.utils;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class IntegerGeneratorIterator implements Iterator<Integer> {
	
	private final int size;
	private final AtomicInteger counter = new AtomicInteger(0);
	
	public IntegerGeneratorIterator(int size) {
		this.size = size;
	}
	
	@Override
	public boolean hasNext() {
		return counter.get() < size;
	}
	
	@Override
	public Integer next() {
		return counter.getAndIncrement();
	}
	
	public Stream<Integer> toStream() {
		return StreamSupport.stream(Spliterators.spliteratorUnknownSize(this, Spliterator.ORDERED), false);
	}
}
