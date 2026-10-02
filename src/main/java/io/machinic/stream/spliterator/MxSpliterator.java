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

import java.util.function.Consumer;

/**
 * The MxSpliterator interface provides the structure of the Spliterator implementations used by {code MxStream}. This implementation is similar to but different from the {code java.util.Spliterator} since MxStream sources are assumed to have infinite number of items.
 */
public interface MxSpliterator<T> {
	
	/**
	 * If a remaining element exists: performs the given action on it, returning {@code true}; else returns {@code false}.  All MxSpliterator instances are assumed to be ordered unless an operation such as {code MxStream.fanOut} is used which will break order.  Exceptions thrown by the
	 * action are relayed to the caller.
	 * @param action The action whose operation is performed at-most once
	 * @return {@code false} if no remaining elements existed upon entry to this method, else {@code true}.
	 */
	boolean tryAdvance(Consumer<? super T> action);
	
	/**
	 * Performs the given action for each remaining element, sequentially in the current thread, until all elements have been processed or the action throws an exception.
	 * @param action The action
	 * @throws NullPointerException if the specified action is null
	 */
	default void forEachRemaining(Consumer<? super T> action) {
		do {
		} while (tryAdvance(action));
	}
	
	/**
	 * If this spliterator can be partitioned, returns a Spliterator covering elements, that will, upon return from this method, not be covered by this Spliterator.
	 *
	 * <p>This method may return {@code null} for any reason,
	 * including emptiness, inability to split after traversal has
	 * commenced, data structure constraints, and efficiency
	 * considerations.
	 * @return a {@code MxSpliterator} covering some portion of the elements, or {@code null} if this spliterator cannot be split
	 */
	MxSpliterator<T> trySplit();
	
	void close();
	
}
