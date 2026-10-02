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

import io.machinic.stream.FlatMapProducerFunction;
import io.machinic.stream.MxStream;
import io.machinic.stream.StreamEventException;
import io.machinic.stream.StreamException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;
import java.util.function.Supplier;

public class FlatMapSpliterator<IN, OUT> extends AbstractChainedSpliterator<IN, OUT> {
	private static final Logger logger = LoggerFactory.getLogger(FlatMapSpliterator.class);
	
	private final Supplier<FlatMapProducerFunction<? super IN, ? extends OUT>> supplier;
	
	public FlatMapSpliterator(MxStream<IN> stream, MxSpliterator<IN> previousSpliterator, Supplier<FlatMapProducerFunction<? super IN, ? extends OUT>> supplier) {
		super(stream, previousSpliterator);
		this.supplier = supplier;
	}
	
	@Override
	public boolean tryAdvance(Consumer<? super OUT> action) {
		return this.previousSpliterator.tryAdvance(value -> {
			try {
				supplier.get().apply(value, action);
			} catch (StreamEventException e) {
				logger.info("Ignoring StreamEventException: {}", e.getMessage(), e);
			} catch (StreamException e) {
				throw e;
			} catch (Exception e) {
				stream.exceptionHandler().onException(e, value);
			}
		});
	}
	
	@Override
	public AbstractChainedSpliterator<IN, OUT> split(MxSpliterator<IN> spliterator) {
		return new FlatMapSpliterator<>(this.stream, spliterator, supplier);
	}
	
}
