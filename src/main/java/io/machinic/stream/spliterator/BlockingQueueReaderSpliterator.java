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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class BlockingQueueReaderSpliterator<T> extends AbstractSpliterator<T, T> {
	
	private final BlockingQueue<QueueWrapper<T>> queue;
	
	// Create a Spliterator that reads from a BlockingQueue. The previousSpliterator is used for characteristics.
	public BlockingQueueReaderSpliterator(MxStream<T> stream, boolean parallel, BlockingQueue<QueueWrapper<T>> queue) {
		super(stream, parallel);
		this.queue = queue;
	}
	
	@Override
	public boolean tryAdvance(Consumer<? super T> action) {
		try {
			QueueWrapper<T> next = null;
			do {
				next = queue.poll(50, TimeUnit.MILLISECONDS);
				if (next != null) {
					action.accept(next.get());
				} else //noinspection ConstantValue
					if (next == null) {
						return !stream.isClosed();
					}
			} while (true);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	protected MxSpliterator<T> split() {
		return new BlockingQueueReaderSpliterator<>(this.stream, parallel, queue);
	}
	
	public static class QueueWrapper<T> {
		private final T value;
		
		public QueueWrapper(T value) {
			this.value = value;
		}
		
		public T get() {
			return value;
		}
	}
}
