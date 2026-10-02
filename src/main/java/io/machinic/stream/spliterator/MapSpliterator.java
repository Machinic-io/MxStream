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

package io.machinic.stream.spliterator;

import io.machinic.stream.MxStream;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class MapSpliterator<IN, OUT> extends AbstractChainedSpliterator<IN, OUT> {
	
	private final Supplier<Function<? super IN, ? extends OUT>> supplier;
	private final Function<? super IN, ? extends OUT> mapper;
	
	public MapSpliterator(MxStream<IN> stream, MxSpliterator<IN> previousSpliterator, Supplier<Function<? super IN, ? extends OUT>> supplier) {
		super(stream, previousSpliterator);
		this.supplier = Objects.requireNonNull(supplier);
		this.mapper = supplier.get();
	}
	
	@Override
	public boolean tryAdvance(Consumer<? super OUT> action) {
		return this.previousSpliterator.tryAdvance(value -> {
			try {
				action.accept(mapper.apply(value));
			} catch (Exception e) {
				stream.exceptionHandler().onException(e, value);
			}
		});
	}
	
	@Override
	public AbstractChainedSpliterator<IN, OUT> split(MxSpliterator<IN> spliterator) {
		return new MapSpliterator<>(this.stream, spliterator, supplier);
	}
	
}
