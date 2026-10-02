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
import io.machinic.stream.StreamException;

import java.util.function.Consumer;
import java.util.function.Supplier;

public class PeekSpliterator<T> extends AbstractChainedSpliterator<T, T> {
	
	private final Supplier<Consumer<? super T>> supplier;
	private final Consumer<? super T> consumer;
	
	public PeekSpliterator(MxStream<T> stream, MxSpliterator<T> previousSpliterator, Supplier<Consumer<? super T>> supplier) {
		super(stream, previousSpliterator);
		this.supplier = supplier;
		this.consumer = supplier.get();
	}
	
	@Override
	public boolean tryAdvance(Consumer<? super T> action) {
		return this.previousSpliterator.tryAdvance(value -> {
			try {
				this.consumer.accept(value);
				action.accept(value);
			} catch (StreamException e) {
				throw e;
			} catch (Exception e) {
				stream.exceptionHandler().onException(e, value);
				action.accept(value);
			}
		});
	}
	
	@Override
	public MxSpliterator<T> split(MxSpliterator<T> spliterator) {
		return new PeekSpliterator<>(this.stream, spliterator, supplier);
	}
}
