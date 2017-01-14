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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
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
import com.srotya.sidewinder.core.storage.ItemNotFoundException;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.TimeUtils;

/**
 * Unit tests for {@link MemStorageEngine}
 * 
 * @author ambud
 */
public class TestMemStorageEngine {

	private Map<String, String> conf = new HashMap<>();

	@Test
	public void testTagEncodeDecode() throws IOException {
		MemStorageEngine engine = new MemStorageEngine();
		engine.configure(new HashMap<>());
		String encodedStr = engine.encodeTagsToString(Arrays.asList("host", "value", "test"));

		List<String> decodedStr = engine.decodeStringToTags(encodedStr);

		System.out.println(decodedStr);
	}

	@Test
	public void testConfigure() {
		StorageEngine engine = new MemStorageEngine();
		try {
			engine.writeDataPoint("test",
					new DataPoint("test", "ss", "value", Arrays.asList("te"), System.currentTimeMillis(), 2.2));
			fail("Engine not initialized, shouldn't be able to write a datapoint");
		} catch (Exception e) {
		}

		try {
			engine.configure(new HashMap<>());
		} catch (IOException e) {
			fail("No IOException should be thrown");
		}
		try {
			engine.writeDataPoint("test",
					new DataPoint("test", "ss", "value", Arrays.asList("te"), System.currentTimeMillis(), 2.2));
		} catch (Exception e) {
			e.printStackTrace();
			fail("Engine is initialized, no IO Exception should be thrown:" + e.getMessage());
		}
	}

	@Test
	public void testQueryDataPoints() throws IOException, ItemNotFoundException {
		StorageEngine engine = new MemStorageEngine();
		engine.configure(conf);
		long ts = System.currentTimeMillis();
		engine.writeSeries("test", "cpu", Arrays.asList("test"), TimeUnit.MILLISECONDS, ts, 1, null);
		engine.writeSeries("test", "cpu", Arrays.asList("test"), TimeUnit.MILLISECONDS, ts + (400 * 60000), 4, null);
		List<DataPoint> queryDataPoints = engine.queryDataPoints("test", "cpu", ts, ts + (400 * 60000), null, null);
		assertEquals(2, queryDataPoints.size());
		assertEquals(ts, queryDataPoints.get(0).getTimestamp());
		assertEquals(ts + (400 * 60000), queryDataPoints.get(1).getTimestamp());
	}

	@Test
	public void testGetMeasurementsLike() throws IOException {
		StorageEngine engine = new MemStorageEngine();
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
	public void testSeriesToDataPointConversion() throws RejectException {
		List<DataPoint> points = new ArrayList<>();
		long headerTimestamp = System.currentTimeMillis();
		TimeSeriesBucket timeSeries = new TimeSeriesBucket(headerTimestamp);
		timeSeries.addDataPoint(headerTimestamp, 1L);
		TimeSeries.seriesToDataPoints(Arrays.asList("test"), points, timeSeries, null, null, false);
		assertEquals(1, points.size());
		points.clear();

		Predicate timepredicate = new BetweenPredicate(Long.MAX_VALUE, Long.MAX_VALUE);
		TimeSeries.seriesToDataPoints(Arrays.asList("test"), points, timeSeries, timepredicate, null, false);
		assertEquals(0, points.size());
	}

	@Test
	public void testSeriesBucketLookups() throws IOException, ItemNotFoundException {
		MemStorageEngine engine = new MemStorageEngine();
		engine.configure(new HashMap<>());
		engine.connect();
		String dbName = "test1";
		String measurementName = "cpu";
		List<String> tags = Arrays.asList("test");

		long ts = 1483923600000L;
		System.out.println("Base timestamp=" + new Date(ts));

		for (int i = 0; i < 100; i++) {
			engine.writeSeries(dbName, measurementName, tags, TimeUnit.MILLISECONDS, ts + (i * 60000), 2.2, null);
		}
		System.out.println("Buckets:" + engine.getSeriesMap(dbName, measurementName).size());
		long endTs = ts + 99 * 60000;

		// validate all points are returned with a full range query
		List<DataPoint> points = engine.queryDataPoints(dbName, measurementName, ts, endTs, tags, null);
		assertEquals(ts, points.get(0).getTimestamp());
		assertEquals(endTs, points.get(points.size() - 1).getTimestamp());

		// validate ts-1 yields the same result
		points = engine.queryDataPoints(dbName, measurementName, ts - 1, endTs, tags, null);
		assertEquals(ts, points.get(0).getTimestamp());
		assertEquals(endTs, points.get(points.size() - 1).getTimestamp());

		// validate ts+1 yields correct result
		points = engine.queryDataPoints(dbName, measurementName, ts + 1, endTs, tags, null);
		assertEquals(ts + 60000, points.get(0).getTimestamp());
		assertEquals(endTs, points.get(points.size() - 1).getTimestamp());

		// validate that points have been written to 2 different buckets
		assertTrue(TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, ts, 4096) != TimeUtils
				.getTimeBucket(TimeUnit.MILLISECONDS, endTs, 4096));
		// calculate base timestamp for the second bucket
		long baseTs2 = ((long) TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, endTs, 4096)) * 1000;
		System.out.println("Bucket2 base timestamp=" + new Date(baseTs2));

		// validate random seek with deliberate time offset
		points = engine.queryDataPoints(dbName, measurementName, ts, baseTs2, tags, null);
		assertEquals("Invalid first entry:" + new Date(points.get(0).getTimestamp()), ts, points.get(0).getTimestamp());
		assertEquals("Invalid first entry:" + (baseTs2 - ts), (baseTs2 / 60000) * 60000,
				points.get(points.size() - 1).getTimestamp());

		points = engine.queryDataPoints(dbName, measurementName, baseTs2, endTs, tags, null);
		assertEquals("Invalid first entry:" + new Date(points.get(0).getTimestamp()), (baseTs2 - ts),
				(baseTs2 / 60000) * 60000, points.get(0).getTimestamp());
		assertEquals("Invalid first entry:" + endTs, endTs, points.get(points.size() - 1).getTimestamp());

		// validate correct results when time range is incorrectly swapped i.e.
		// end time is smaller than start time
		points = engine.queryDataPoints(dbName, measurementName, endTs - 1, baseTs2, tags, null);
		assertEquals("Invalid first entry:" + new Date(points.get(0).getTimestamp()), (baseTs2 - ts),
				(baseTs2 / 60000) * 60000, points.get(0).getTimestamp());
		assertEquals("Invalid first entry:" + endTs, endTs - 60000, points.get(points.size() - 1).getTimestamp());
	}

	@Test
	public void testBaseTimeSeriesWrites() throws Exception {
		MemStorageEngine engine = new MemStorageEngine();
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
