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
import io.machinic.stream.StreamInterruptedException;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class FanOutSpliterator<T> extends AbstractChainedSpliterator<T, T> {
	
	private final BlockingQueue<Wrapper> queue;
	private volatile boolean done = false;
	
	public FanOutSpliterator(MxStream<T> stream, MxSpliterator<T> previousSpliterator, int bufferSize) {
		super(stream, previousSpliterator);
		this.queue = new ArrayBlockingQueue<>(bufferSize);
	}
	
	protected boolean isDone() {
		return this.done;
	}
	
	@Override
	public boolean tryAdvance(Consumer<? super T> action) {
		boolean advance = true;
		do {
			advance = this.previousSpliterator.tryAdvance(value -> {
				try {
					do {
						if (queue.offer(new Wrapper(value), 100, TimeUnit.MILLISECONDS)) {
							break;
						}
					} while (!done);
				} catch (InterruptedException e) {
					this.close();
				}
			});
		} while (!done && advance);
		this.done = true;
		return false;
	}
	
	@Override
	protected MxSpliterator<T> split(MxSpliterator<T> spliterator) {
		// not used for this implementation
		throw new UnsupportedOperationException();
	}
	
	@Override
	public MxSpliterator<T> trySplit() {
		return new FanOutSecondarySpliterator(this);
	}
	
	private class Wrapper {
		private final T value;
		
		public Wrapper(T value) {
			this.value = value;
		}
		
		public T getValue() {
			return value;
		}
	}
	
	@Override
	public void close() {
		this.done = true;
		this.queue.clear();
	}
	
	public class FanOutSecondarySpliterator implements MxSpliterator<T> {
		
		private final FanOutSpliterator<T> parent;
		
		public FanOutSecondarySpliterator(FanOutSpliterator<T> parent) {
			this.parent = parent;
		}
		
		@Override
		public boolean tryAdvance(Consumer<? super T> action) {
			Wrapper wrapper = null;
			try {
				do {
					wrapper = parent.queue.poll(100, TimeUnit.MILLISECONDS);
					if (wrapper != null) {
						action.accept(wrapper.getValue());
					}
				} while (wrapper != null);
				return !this.parent.isDone();
			} catch (StreamException e) {
				throw e;
			} catch (RuntimeException e) {
				parent.getStream().exceptionHandler().onException(e, (wrapper != null ? wrapper.getValue() : null));
			} catch (InterruptedException e) {
				throw new StreamInterruptedException("FanOutSpliterator was interrupted");
			}
			return false;
		}
		
		@Override
		public MxSpliterator<T> trySplit() {
			return parent.trySplit();
		}
		
		@Override
		public void close() {
			parent.close();
		}
	}
}
