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

package io.machinic.stream.test;

import io.machinic.stream.MxStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * Race-condition reproducer: async tasks emit far more outputs than the internal buffer can hold,
 * so producer threads spend most of their time blocked in bufferQueue.put() while the stream
 * thread's queue is saturated. Under the pre-fix code the stream thread's wait/drain paths
 * livelock or hang here; the fixed code must always drain the buffer while waiting.
 */
public class AsyncFlatMapRaceReproTest {
	
	private List<Integer> runWithTimeout(java.util.concurrent.Callable<List<Integer>> streamOp, long timeoutSeconds) throws Exception {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		try {
			Future<List<Integer>> future = executor.submit(streamOp);
			return future.get(timeoutSeconds, TimeUnit.SECONDS);
		} finally {
			executor.shutdownNow();
		}
	}
	
	@Test
	public void saturatedBufferHighVolumeTest() throws Exception {
		// 400 inputs, parallelism 3, buffer 4, each task emits 30 elements (>> buffer size)
		List<Integer> result = runWithTimeout(() ->
				MxStream.of(IntStream.rangeClosed(1, 400).boxed().toList())
						.asyncFlatMap(3, 4, integer -> IntStream.range(0, 30).boxed().toList().stream())
						.toList(), 30);
		Assertions.assertEquals(400 * 30, result.size());
	}
	
	@Test
	public void saturatedBufferWithProducerWorkTest() throws Exception {
		// same shape but each output involves a tiny sleep so the scheduler interleaves aggressively
		List<Integer> result = runWithTimeout(() ->
				MxStream.of(IntStream.rangeClosed(1, 200).boxed().toList())
						.asyncFlatMap(3, 4, integer -> {
							return IntStream.range(0, 20).peek(v -> {
								try {
									Thread.sleep(0, 50_000); // 0.05 ms per emitted element
								} catch (InterruptedException e) {
									Thread.currentThread().interrupt();
								}
							}).boxed().toList().stream();
						})
						.toList(), 30);
		Assertions.assertEquals(200 * 20, result.size());
	}
	
}
