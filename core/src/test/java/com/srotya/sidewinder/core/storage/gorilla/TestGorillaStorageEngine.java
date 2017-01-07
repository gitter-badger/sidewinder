/**
 * Copyright 2016 Ambud Sharma
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.srotya.sidewinder.core.storage.gorilla;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.srotya.sidewinder.core.predicates.BetweenPredicate;
import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * Unit tests for {@link GorillaStorageEngine}
 * 
 * @author ambud
 */
public class TestGorillaStorageEngine {

	private Map<String, String> conf = new HashMap<>();

	@Test
	public void testConfigure() {
		StorageEngine engine = new GorillaStorageEngine();
		try {
			engine.writeDataPoint("test", new DataPoint("ss", Arrays.asList("te"), System.currentTimeMillis(), 2.2));
			fail("Engine not initialized, shouldn't be able to write a datapoint");
		} catch (Exception e) {
		}

		try {
			engine.configure(new HashMap<>());
		} catch (IOException e) {
			fail("No IOException should be thrown");
		}
		try {
			engine.writeDataPoint("test", new DataPoint("ss", Arrays.asList("te"), System.currentTimeMillis(), 2.2));
		} catch (Exception e) {
			fail("Engine is initialized, no IO Exception should be thrown");
		}
	}

	@Test
	public void testQueryDataPoints() throws IOException {
		StorageEngine engine = new GorillaStorageEngine();
		engine.configure(conf);
		long ts = System.currentTimeMillis();
		engine.writeSeries("test", "cpu", null, TimeUnit.MILLISECONDS, ts, 1, null);
		engine.writeSeries("test", "cpu", null, TimeUnit.MILLISECONDS, ts + (400 * 60000), 4, null);
		List<DataPoint> queryDataPoints = engine.queryDataPoints("test", "cpu", ts, ts + (400 * 60000), null, null);
		assertEquals(2, queryDataPoints.size());
		assertEquals(ts, queryDataPoints.get(0).getTimestamp());
		assertEquals(ts + (400 * 60000), queryDataPoints.get(1).getTimestamp());
	}

	@Test
	public void testGetMeasurementsLike() throws IOException {
		StorageEngine engine = new GorillaStorageEngine();
		engine.configure(conf);
		engine.writeSeries("test", "cpu", Arrays.asList("test"), TimeUnit.MILLISECONDS, System.currentTimeMillis(), 2L,
				null);
		engine.writeSeries("test", "mem", Arrays.asList("test"), TimeUnit.MILLISECONDS, System.currentTimeMillis() + 10,
				3L, null);
		engine.writeSeries("test", "netm", Arrays.asList("test"), TimeUnit.MILLISECONDS,
				System.currentTimeMillis() + 20, 5L, null);
		Set<String> result = engine.getMeasurementsLike("test", " ");
		assertEquals(3, result.size());

		result = engine.getMeasurementsLike("test", "c");
		assertEquals(1, result.size());

		result = engine.getMeasurementsLike("test", "m");
		assertEquals(2, result.size());
	}

	@Test
	public void testSeriesToDataPointConversion() {
		List<DataPoint> points = new ArrayList<>();
		long headerTimestamp = System.currentTimeMillis();
		TimeSeries timeSeries = new TimeSeries(false, headerTimestamp);
		timeSeries.addDatapoint(headerTimestamp, 1L);
		GorillaStorageEngine.seriesToDataPoints(points, timeSeries, null, null);
		assertEquals(1, points.size());
		points.clear();

		Predicate timepredicate = new BetweenPredicate(Long.MAX_VALUE, Long.MAX_VALUE);
		GorillaStorageEngine.seriesToDataPoints(points, timeSeries, timepredicate, null);
		assertEquals(0, points.size());
	}

	@Test
	public void testBaseTimeSeriesWrites() throws Exception {
		GorillaStorageEngine engine = new GorillaStorageEngine();
		engine.configure(new HashMap<>());
		engine.connect();

		final long ts1 = System.currentTimeMillis();

		ExecutorService es = Executors.newCachedThreadPool();

		for (int k = 0; k < 500; k++) {
			final int p = k;
			es.submit(() -> {
				long ts = System.currentTimeMillis();
				for (int i = 0; i < 1000; i++) {
					try {
						engine.writeSeries("test", "helo" + p, Arrays.asList(""), TimeUnit.MILLISECONDS, ts + i * 60,
								ts + i, null);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			});
		}
		es.shutdown();
		es.awaitTermination(10, TimeUnit.SECONDS);

		System.out.println("Write time:" + (System.currentTimeMillis() - ts1) + "\tms");
	}

}
