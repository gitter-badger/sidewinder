/**
 * Copyright 2017 Ambud Sharma
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import com.srotya.sidewinder.core.PerformantException;
import com.srotya.sidewinder.core.predicates.BetweenPredicate;
import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.Callback;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.TimeUtils;

/**
 * @author ambud
 */
public class GorillaStorageEngine implements StorageEngine {

	private static final int TIME_BUCKET_CONSTANT = 4096;
	private static final Logger logger = Logger.getLogger(GorillaStorageEngine.class.getName());
	private static PerformantException INVALID_DATAPOINT_EXCEPTION = new PerformantException();
	private Map<String, SortedMap<String, SortedMap<String, TimeSeries>>> databaseMap;
	private AtomicInteger counter = new AtomicInteger(0);

	@Override
	public void configure(Map<String, String> conf) throws IOException {
		databaseMap = new ConcurrentHashMap<>();
		Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
			// System.out.println(counter.getAndSet(0));
		}, 0, 1, TimeUnit.SECONDS);
	}

	@Override
	public List<DataPoint> queryDataPoints(String dbName, String measurementName, long startTime, long endTime,
			List<String> tags, Predicate valuePredicate) {
		List<DataPoint> points = new ArrayList<>();
		SortedMap<String, SortedMap<String, TimeSeries>> measurementMap = databaseMap.get(dbName);
		if (measurementMap != null) {
			SortedMap<String, TimeSeries> seriesMap = measurementMap.get(measurementName);
			if (seriesMap != null) {
				BetweenPredicate timeRangePredicate = null;// new
															// BetweenPredicate(startTime,
															// endTime);
				int tsStartBucket = TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, startTime, TIME_BUCKET_CONSTANT);
				String startTsBucket = Integer.toHexString(tsStartBucket);
				int tsEndBucket = TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, endTime, TIME_BUCKET_CONSTANT);
				String endTsBucket = Integer.toHexString(tsEndBucket);
				SortedMap<String, TimeSeries> series = seriesMap.subMap(startTsBucket,
						endTsBucket + Character.MAX_VALUE);
				if (series == null || series.isEmpty()) {
					TimeSeries timeSeries = seriesMap.get(startTsBucket);
					if (timeSeries != null) {
						seriesToDataPoints(points, timeSeries, timeRangePredicate, valuePredicate);
					}
				} else {
					for (TimeSeries timeSeries : series.values()) {
						seriesToDataPoints(points, timeSeries, timeRangePredicate, valuePredicate);
						System.out.println("Count of points:" + timeSeries.getCount() + "\t" + points.size());
					}
				}
			} else {
				System.out.println("Measurement not found:" + measurementName);
			}
		} else {
			System.out.println("DB not found:" + dbName);
		}
		return points;
	}

	/**
	 * Converts timeseries to a list of datapoints appended to the supplied list
	 * object. Datapoints are filtered by the supplied predicates before they
	 * are returned. These predicates are pushed down to the reader for
	 * efficiency and performance as it prevents unnecessary object creation.
	 * 
	 * @param points
	 *            list data points are appended to
	 * @param timeSeries
	 *            to extract the data points from
	 * @param timePredicate
	 *            time range filter
	 * @param valuePredicate
	 *            value filter
	 * @return the points argument
	 */
	public static List<DataPoint> seriesToDataPoints(List<DataPoint> points, TimeSeries timeSeries, Predicate timePredicate,
			Predicate valuePredicate) {
		Reader reader = timeSeries.getReader(timePredicate, valuePredicate);
		DataPoint point = null;
		while (true) {
			try {
				point = reader.readPair();
				if (point != null) {
					points.add(point);
				}
			} catch (IOException e) {
				break;
			}
		}
		return points;
	}

	@Override
	public Set<String> getMeasurementsLike(String dbName, String partialMeasurementName) throws IOException {
		SortedMap<String, SortedMap<String, TimeSeries>> measurementMap = databaseMap.get(dbName);
		partialMeasurementName = partialMeasurementName.trim();
		if (partialMeasurementName.isEmpty()) {
			return measurementMap.keySet();
		} else {
			Set<String> filteredSeries = new HashSet<>();
			for (String measurementName : measurementMap.keySet()) {
				if (measurementName.contains(partialMeasurementName)) {
					filteredSeries.add(measurementName);
				}
			}
			return filteredSeries;
		}
	}

	@Override
	public void writeDataPoint(String dbName, DataPoint dp) throws IOException {
		TimeSeries timeSeries = getOrCreateTimeSeries(dbName, dp.getSeriesName(), dp.getTags(), TimeUnit.NANOSECONDS,
				dp.getTimestamp(), dp.isFp());
		if (dp.isFp() != timeSeries.isFp()) {
			// drop this datapoint, mixed series are not allowed
			throw INVALID_DATAPOINT_EXCEPTION;
		}
		if (dp.isFp()) {
			timeSeries.addDatapoint(dp.getTimestamp(), dp.getValue());
		} else {
			timeSeries.addDatapoint(dp.getTimestamp(), dp.getLongValue());
		}
		counter.incrementAndGet();
	}

	@Override
	public void writeSeries(String dbName, String measurementName, List<String> tags, TimeUnit unit, long timestamp,
			long value, Callback callback) throws IOException {
		TimeSeries timeSeries = getOrCreateTimeSeries(dbName, measurementName, tags, unit, timestamp, false);
		timeSeries.addDatapoint(timestamp, value);
		counter.incrementAndGet();
	}

	protected TimeSeries getOrCreateTimeSeries(String dbName, String measurementName, List<String> tags, TimeUnit unit,
			long timestamp, boolean fp) {
		int bucket = TimeUtils.getTimeBucket(unit, timestamp, TIME_BUCKET_CONSTANT);
		String tsBucket = Integer.toHexString(bucket);
		// StringBuilder builder = new StringBuilder(measurementName.length() +
		// 1 + tsBucket.length());
		// builder.append(tsBucket);
		String rowKey = tsBucket;// builder.toString();

		// check and create database map
		SortedMap<String, SortedMap<String, TimeSeries>> measurementMap = databaseMap.get(dbName);
		if (measurementMap == null) {
			measurementMap = new ConcurrentSkipListMap<>();
			databaseMap.put(dbName, measurementMap);
			logger.fine("Created new database:" + dbName);
		}

		// check and create measurement map
		SortedMap<String, TimeSeries> seriesMap = measurementMap.get(measurementName);
		if (seriesMap == null) {
			seriesMap = new ConcurrentSkipListMap<>();
			measurementMap.put(measurementName, seriesMap);
			logger.fine("Created new measurement:" + measurementName);
		}

		// check and create timeseries
		TimeSeries timeSeries = seriesMap.get(rowKey);
		if (timeSeries == null) {
			timeSeries = new TimeSeries(fp, timestamp);
			seriesMap.put(rowKey, timeSeries);
			logger.fine("Created new timeseries:" + timeSeries);
		}
		return timeSeries;
	}

	@Override
	public void writeSeries(String dbName, String measurementName, List<String> tags, TimeUnit unit, long timestamp,
			double value, Callback callback) throws IOException {
		TimeSeries timeSeries = getOrCreateTimeSeries(dbName, measurementName, tags, unit, timestamp, true);
		timeSeries.addDatapoint(timestamp, value);
		counter.incrementAndGet();
	}

	@Override
	public Set<String> getDatabases() throws Exception {
		return databaseMap.keySet();
	}

	@Override
	public Set<String> getAllMeasurementsForDb(String dbName) throws Exception {
		return databaseMap.get(dbName).keySet();
	}

	@Override
	public void deleteAllData() throws Exception {
		// Extremely dangerous operation
		databaseMap.clear();
	}

	@Override
	public boolean checkIfExists(String dbName) throws Exception {
		return databaseMap.containsKey(dbName);
	}

	@Override
	public void dropDatabase(String dbName) throws Exception {
		databaseMap.remove(dbName);
	}

	@Override
	public void connect() throws IOException {
	}

	@Override
	public void disconnect() throws IOException {
	}

}
