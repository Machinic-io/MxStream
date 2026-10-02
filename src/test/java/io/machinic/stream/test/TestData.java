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

import io.machinic.stream.MxStreamExceptionHandler;
import io.machinic.stream.StreamException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TestData {
	
	public static final List<Integer> INTEGER_LIST_A = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
	public static final Set<Integer> INTEGER_SET_A = new HashSet<>(INTEGER_LIST_A);
	public static final List<String> STRING_LIST_A = INTEGER_LIST_A.stream().map(String::valueOf).collect(Collectors.toList());
	public static final Set<String> STRING_SET_A = new HashSet<>(STRING_LIST_A);
	public static final String STRING_A = INTEGER_LIST_A.stream().map(String::valueOf).collect(Collectors.joining("\n"));
	
	public static final List<Integer> INTEGER_LIST_B = INTEGER_LIST_A.stream().filter(value -> value % 2 == 0).toList();
	public static final Set<Integer> INTEGER_SET_B = new HashSet<>(INTEGER_LIST_B);
	public static final List<String> STRING_LIST_B = INTEGER_LIST_B.stream().map(String::valueOf).collect(Collectors.toList());
	public static final Set<String> STRING_SET_B = new HashSet<>(STRING_LIST_B);
	
	public static final List<List<Integer>> INTEGER_LIST_C = INTEGER_LIST_A.stream().map(List::of).toList();
	
	public static final List<List<Integer>> INTEGER_LIST_D = List.of(List.of(1, 2, 3, 4, 5), List.of(6, 7, 8, 9, 10));
	public static final Set<List<Integer>> INTEGER_SET_D = new HashSet<>(INTEGER_LIST_D);
	
	public static final List<List<Integer>> INTEGER_LIST_E = INTEGER_LIST_A.stream().map(List::of).toList();
	public static final Set<List<Integer>> INTEGER_SET_E = INTEGER_LIST_A.stream().map(List::of).collect(Collectors.toSet());
	
	public static final MxStreamExceptionHandler NOOP_EXCEPTION_HANDLER = new MxStreamExceptionHandler() {
		@Override
		public void onException(Exception e, Object value) throws StreamException {
			// NOOP on exception
		}
	};
}
