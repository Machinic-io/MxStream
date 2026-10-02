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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class BatchSpliterator<T> extends AbstractChainedSpliterator<T, List<T>> {
	
	private final int batchSize;
	private final AtomicReference<List<T>> batchReference;
	
	public BatchSpliterator(MxStream<T> stream, MxSpliterator<T> previousSpliterator, int batchSize) {
		super(stream, previousSpliterator);
		this.batchSize = batchSize;
		batchReference = new AtomicReference<>(new ArrayList<>(batchSize));
	}
	
	@Override
	public boolean tryAdvance(Consumer<? super List<T>> action) {
		if (this.previousSpliterator.tryAdvance(value -> {
			List<T> batch = batchReference.getPlain();
			batch.add(value);
			// Push batch if full
			if (batch.size() >= batchSize) {
				action.accept(batch);
				batchReference.setPlain(new ArrayList<>(batchSize));
			}
		})) {
			return true;
		} else {
			if (!batchReference.getPlain().isEmpty()) {
				action.accept(batchReference.getPlain());
				batchReference.setPlain(new ArrayList<>(batchSize));
			}
			return false;
		}
	}
	
	@Override
	public MxSpliterator<List<T>> split(MxSpliterator<T> spliterator) {
		return new BatchSpliterator<>(this.stream, spliterator, batchSize);
	}
	
}
