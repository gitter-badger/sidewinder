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
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.Callback;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.ItemNotFoundException;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * In-memory Timeseries {@link StorageEngine} implementation that uses the
 * following hierarchy:
 * <ul>
 * <li>Database</li>
 * <ul>
 * <li>Measurement</li>
 * <ul>
 * <li>Time Series</li>
 * </ul>
 * </ul>
 * </ul>
 * 
 * {@link TimeSeriesBucket} is uses compressed in-memory representation of the
 * actual data. Periodic checks against size ensure that Sidewinder server
 * doesn't run out of memory. Each timeseries has a <br>
 * <br>
 * 
 * 
 * 
 * @author ambud
 */
public class MemStorageEngine implements StorageEngine {

	private static final Logger logger = Logger.getLogger(MemStorageEngine.class.getName());
	private static RejectException INVALID_DATAPOINT_EXCEPTION = new RejectException();
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
			List<String> tags, Predicate valuePredicate) throws ItemNotFoundException {
		if (startTime > endTime) {
			// swap start and end times if they are off
			startTime = startTime ^ endTime;
			endTime = endTime ^ startTime;
			startTime = startTime ^ endTime;
		}
		List<DataPoint> points = new ArrayList<>();
		SortedMap<String, SortedMap<String, TimeSeries>> measurementMap = databaseMap.get(dbName);
		if (measurementMap != null) {
			SortedMap<String, TimeSeries> seriesMap = measurementMap.get(measurementName);
			if (seriesMap != null) {
				for (Entry<String, TimeSeries> entry : seriesMap.entrySet()) {
					points.addAll(entry.getValue().queryDataPoints(startTime, endTime, valuePredicate));
				}
			} else {
				throw new ItemNotFoundException("Measurement " + measurementName + " not found");
			}
		} else {
			throw new ItemNotFoundException("Database " + dbName + " not found");
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
		TimeSeries timeSeries = getOrCreateTimeSeries(dbName, dp.getMeasurementName(), dp.getTags(),
				TimeUnit.MILLISECONDS, dp.getTimestamp(), dp.isFp());
		if (dp.isFp() != timeSeries.isFp()) {
			// drop this datapoint, mixed series are not allowed
			throw INVALID_DATAPOINT_EXCEPTION;
		}
		if (dp.isFp()) {
			timeSeries.addDataPoint(TimeUnit.MILLISECONDS, dp.getTimestamp(), dp.getValue());
		} else {
			timeSeries.addDataPoint(TimeUnit.MILLISECONDS, dp.getTimestamp(), dp.getLongValue());
		}
		counter.incrementAndGet();
	}

	@Override
	public void writeSeries(String dbName, String measurementName, List<String> tags, TimeUnit unit, long timestamp,
			long value, Callback callback) throws IOException {
		TimeSeries timeSeries = getOrCreateTimeSeries(dbName, measurementName, tags, unit, timestamp, false);
		timeSeries.addDataPoint(unit, timestamp, value);
		counter.incrementAndGet();
	}

	protected TimeSeries getOrCreateTimeSeries(String dbName, String measurementName, List<String> tags, TimeUnit unit,
			long timestamp, boolean fp) {
		String rowKey = "default";// builder.toString(); // replace with tags

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
			timeSeries = new TimeSeries(fp);
			seriesMap.put(rowKey, timeSeries);
			logger.fine("Created new timeseries:" + timeSeries + " for measurement:" + measurementName + "\t" + rowKey);
		}
		return timeSeries;
	}

	@Override
	public void writeSeries(String dbName, String measurementName, List<String> tags, TimeUnit unit, long timestamp,
			double value, Callback callback) throws IOException {
		TimeSeries timeSeries = getOrCreateTimeSeries(dbName, measurementName, tags, unit, timestamp, true);
		timeSeries.addDataPoint(unit, timestamp, value);
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
	public void dropMeasurement(String dbName, String measurementName) throws Exception {
		databaseMap.get(dbName).remove(measurementName);
	}

	/**
	 * Function for unit testing
	 * 
	 * @param dbName
	 * @param measurementName
	 * @return
	 */
	protected SortedMap<String, TimeSeries> getSeriesMap(String dbName, String measurementName) {
		return databaseMap.get(dbName).get(measurementName);
	}

	@Override
	public void connect() throws IOException {
	}

	@Override
	public void disconnect() throws IOException {
	}

	@Override
	public boolean checkIfExists(String dbName, String measurement) throws Exception {
		if (checkIfExists(dbName)) {
			return databaseMap.get(dbName).containsKey(measurement);
		} else {
			return false;
		}
	}

}
