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

import io.machinic.stream.spliterator.AbstractChainedSpliterator;

import java.util.concurrent.ExecutorService;

public class PipelineParallel<IN, OUT> extends Pipeline<IN, OUT> {
	
	private final int parallelism;
	private final ExecutorService executorService;
	
	public PipelineParallel(PipelineSource<?> source, BasePipeline<?, IN> previous, int parallelism, ExecutorService executorService, AbstractChainedSpliterator<IN, OUT> spliterator) {
		super(source, previous, spliterator);
		this.parallelism = parallelism;
		this.executorService = (executorService != null ? executorService : previous.getExecutorService());
	}
	
	protected ExecutorService getExecutorService() {
		return executorService;
	}
	
	public boolean isParallel() {
		return true;
	}
	
	public int getParallelism() {
		return parallelism;
	}
	
}
