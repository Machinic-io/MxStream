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

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class MxCollector<T, A, R> implements Supplier<A> {
	
	private final Collector<T, A, R> collector;
	private final List<A> containers = new ArrayList<>();
	
	public MxCollector(Collector<T, A, R> collector) {
		this.collector = collector;
	}
	
	@Override
	public A get() {
		A container = collector.supplier().get();
		containers.add(container);
		return container;
	}
	
	public BiConsumer<A, T> accumulator() {
		return collector.accumulator();
	}
	
	@SuppressWarnings("OptionalGetWithoutIsPresent")
	public R finish() {
		if (containers.size() > 1) {
			return collector.finisher().apply(containers
					.stream()
					.reduce((left, right) -> collector.combiner().apply(left, right))
					.get());
		}
		return collector.finisher().apply(containers.get(0));
	}
}
