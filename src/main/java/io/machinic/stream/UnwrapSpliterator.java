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

import io.machinic.stream.spliterator.MxSpliterator;

import java.util.Spliterator;
import java.util.function.Consumer;

public class UnwrapSpliterator<IN> implements Spliterator<IN> {
	
	private final MxStream<IN> stream;
	private final MxSpliterator<IN> previousSpliterator;
	
	public UnwrapSpliterator(MxStream<IN> stream, MxSpliterator<IN> previousSpliterator) {
		this.stream = stream;
		this.previousSpliterator = previousSpliterator;
	}
	
	@Override
	public boolean tryAdvance(Consumer<? super IN> action) {
		return previousSpliterator.tryAdvance(action);
	}
	
	@Override
	public Spliterator<IN> trySplit() {
		return null;
	}
	
	@Override
	public long estimateSize() {
		return Integer.MAX_VALUE;
	}
	
	@Override
	public int characteristics() {
		if (stream.isParallel()) {
			return (Spliterator.CONCURRENT);
		}
		return 0;
	}
	
	
}