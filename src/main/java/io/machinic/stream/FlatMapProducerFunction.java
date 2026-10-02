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

package io.machinic.stream;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

@FunctionalInterface
public interface FlatMapProducerFunction<T, R> {
	
	void apply(T value, Consumer<? super R> consumer) throws StreamEventException, StreamException;
	
	static <T, R> FlatMapProducerFunction<T, R> wrap(Function<? super T, ? extends Stream<? extends R>> streamMapper) {
		return (value, consumer) -> {
			// Use verbose code to make debugging easier
			try (Stream<? extends R> stream = streamMapper.apply(value)) {
				stream.forEachOrdered(entry -> {
					//noinspection FunctionalExpressionCanBeFolded
					consumer.accept(entry);
				});
			}
		};
	}
}
