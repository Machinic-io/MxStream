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
import io.machinic.stream.spliterator.BlockingQueueReaderSpliterator.QueueWrapper;

import java.util.concurrent.BlockingQueue;

public class BlockingQueueWriterSpliterator<T> extends PeekSpliterator<T> {
	
	private final BlockingQueue<QueueWrapper<T>> queue;
	
	public BlockingQueueWriterSpliterator(MxStream<T> stream, MxSpliterator<T> previousSpliterator, BlockingQueue<QueueWrapper<T>> queue) {
		super(stream, previousSpliterator, () ->
				value -> {
					try {
						queue.put(new QueueWrapper<T>(((T) value)));
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				});
		this.queue = queue;
	}
	
	@Override
	public MxSpliterator<T> split(MxSpliterator<T> spliterator) {
		return new BlockingQueueWriterSpliterator<>(this.stream, spliterator, queue);
	}
}
