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

package io.machinic.stream.util;

import io.machinic.stream.StreamSourceException;

import java.io.BufferedReader;
import java.util.Iterator;

public class BufferedReaderIterator implements Iterator<String> {
	
	private final BufferedReader reader;
	private String next;
	
	public BufferedReaderIterator(BufferedReader reader) {
		this.reader = reader;
	}
	
	@Override
	public boolean hasNext() {
		next = nextLine();
		return next != null;
	}
	
	@Override
	public String next() {
		return next;
	}
	
	private String nextLine() {
		try {
			return reader.readLine();
		} catch (Exception e) {
			throw new StreamSourceException(String.format("Read line from bufferedReader failed: %s", e.getMessage()), e);
		}
	}
	
}
