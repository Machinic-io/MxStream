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

package io.machinic.stream.concurrent;

import io.machinic.stream.FlatMapProducerFunction;

import java.util.function.Consumer;

public final class FlatMapFutureTask<IN, OUT> extends AbstractCallableTask<IN> {
	
	private final IN input;
	private final FlatMapProducerFunction<? super IN, ? extends OUT> flatMapProducerFunction;
	private final Consumer<OUT> consumer;
	
	public FlatMapFutureTask(IN input, FlatMapProducerFunction<? super IN, ? extends OUT> flatMapProducerFunction, Consumer<OUT> consumer) {
		this.input = input;
		this.flatMapProducerFunction = flatMapProducerFunction;
		this.consumer = consumer;
	}
	
	@Override
	public IN callTask() {
		this.flatMapProducerFunction.apply(input, consumer);
		return input;
	}
	
	public IN getInput() {
		return input;
	}
	
}
