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
import io.machinic.stream.metrics.RateStreamMetricSupplier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.machinic.stream.test.TestData.INTEGER_LIST_A;
import static io.machinic.stream.test.TestData.INTEGER_SET_A;

@Execution(ExecutionMode.SAME_THREAD)
public class MxStreamMetricsTest {
	
	private static final Logger LOG = LoggerFactory.getLogger(MxStreamMetricsTest.class);
	
	@BeforeEach
	void setUp(TestInfo testInfo) {
		LOG.info("test started: {}", testInfo.getDisplayName());
	}
	
	@Test
	public void streamMetricsTest() {
		RateStreamMetricSupplier metricSupplier = new RateStreamMetricSupplier();
		Assertions.assertEquals(INTEGER_LIST_A, MxStream.of(INTEGER_LIST_A)
				.metrics(metricSupplier)
				.peek(value -> {
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				})
				.toList());
		Assertions.assertEquals(INTEGER_LIST_A.size(), metricSupplier.getCount());
		Assertions.assertTrue(metricSupplier.getWaitDuration() < 5);
		Assertions.assertTrue(metricSupplier.getDuration() > 50);
		Assertions.assertTrue(metricSupplier.getAverageRate() >= 90D && metricSupplier.getAverageRate() <= 110D, String.format("Average rate is %.2f", metricSupplier.getAverageRate()));
	}
	
	@Test
	public void streamMetricsParallelTest() {
		RateStreamMetricSupplier metricSupplier = new RateStreamMetricSupplier();
		Assertions.assertEquals(INTEGER_SET_A, MxStream.of(INTEGER_LIST_A)
				.fanOut(3, 2)
				.metrics(metricSupplier)
				.peek(value -> {
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				})
				.toSet());
		Assertions.assertEquals(INTEGER_LIST_A.size(), metricSupplier.getCount());
		Assertions.assertTrue(metricSupplier.getDuration() > 50);
		Assertions.assertTrue(metricSupplier.getAverageRate() >= 1D && metricSupplier.getAverageRate() <= 90D, String.format("Average rate is %.2f", metricSupplier.getAverageRate()));
	}
	
	@Test
	public void streamMetricsUpstreamTimeTest() {
		RateStreamMetricSupplier metricSupplier = new RateStreamMetricSupplier();
		Assertions.assertEquals(INTEGER_LIST_A, MxStream.of(INTEGER_LIST_A)
				.peek(value -> {
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				})
				.metrics(metricSupplier)
				.toList());
		Assertions.assertEquals(INTEGER_LIST_A.size(), metricSupplier.getCount());
		Assertions.assertTrue(metricSupplier.getWaitDuration() > 90);
		Assertions.assertTrue(metricSupplier.getDuration() > 50);
		Assertions.assertTrue(metricSupplier.getAverageRate() >= 90D && metricSupplier.getAverageRate() <= 110D, String.format("Average rate is %.2f", metricSupplier.getAverageRate()));
	}
	
}
