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

import java.util.function.Function;

public final class MapFutureTask<IN, OUT> extends AbstractCallableTask<OUT> {
	
	private final Function<? super IN, ? extends OUT> mapper;
	private final IN input;
	
	public MapFutureTask(Function<? super IN, ? extends OUT> mapper, IN input) {
		this.mapper = mapper;
		this.input = input;
	}
	
	@Override
	public OUT callTask() {
		return this.mapper.apply(this.input);
	}
	
	public IN getInput() {
		return input;
	}
	
}
