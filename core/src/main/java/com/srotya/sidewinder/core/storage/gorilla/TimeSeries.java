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
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

import com.srotya.sidewinder.core.predicates.BetweenPredicate;
import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.utils.TimeUtils;

/**
 * A timeseries is defined as a subset of a measurement for a specific set of
 * tags. Measurement is defined as a category and is an abstract to group
 * metrics about a given topic under a the same label. E.g of a measurement is
 * CPU, Memory whereas a {@link TimeSeries} would be cpu measurement on a
 * specific host.<br>
 * <br>
 * Internally a {@link TimeSeries} contains a {@link SortedMap} of buckets that
 * bundle datapoints under temporally sorted partitions that makes storage,
 * retrieval and evictions efficient. This class provides the abstractions
 * around that, therefore partitioning / bucketing interval can be controlled on
 * a per {@link TimeSeries} basis rather than keep it a constant.<br>
 * <br>
 * 
 * @author ambud
 */
public class TimeSeries {

	private static final int TIME_BUCKET_CONSTANT = 4096;
	private SortedMap<String, TimeSeriesBucket> bucketMap;
	private boolean fp;

	public TimeSeries(boolean fp) {
		this.fp = fp;
		bucketMap = new ConcurrentSkipListMap<>();
	}

	public List<DataPoint> queryDataPoints(long startTime, long endTime, Predicate valuePredicate) {
		if (startTime > endTime) {
			// swap start and end times if they are off
			startTime = startTime ^ endTime;
			endTime = endTime ^ startTime;
			startTime = startTime ^ endTime;
		}
		List<DataPoint> points = new ArrayList<>();
		BetweenPredicate timeRangePredicate = new BetweenPredicate(startTime, endTime);
		int tsStartBucket = TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, startTime, TIME_BUCKET_CONSTANT)
				- TIME_BUCKET_CONSTANT;
		String startTsBucket = Integer.toHexString(tsStartBucket);
		int tsEndBucket = TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, endTime, TIME_BUCKET_CONSTANT);
		String endTsBucket = Integer.toHexString(tsEndBucket);
		SortedMap<String, TimeSeriesBucket> series = bucketMap.subMap(startTsBucket, endTsBucket + Character.MAX_VALUE);
		if (series == null || series.isEmpty()) {
			TimeSeriesBucket timeSeries = bucketMap.get(startTsBucket);
			if (timeSeries != null) {
				seriesToDataPoints(points, timeSeries, timeRangePredicate, valuePredicate, fp);
			}
		} else {
			for (TimeSeriesBucket timeSeries : series.values()) {
				seriesToDataPoints(points, timeSeries, timeRangePredicate, valuePredicate, fp);
			}
		}
		return points;
	}

	public void addDataPoint(TimeUnit unit, long timestamp, long value) throws RejectException {
		int bucket = TimeUtils.getTimeBucket(unit, timestamp, TIME_BUCKET_CONSTANT);
		String tsBucket = Integer.toHexString(bucket);
		TimeSeriesBucket timeseriesBucket = bucketMap.get(tsBucket);
		if (timeseriesBucket == null) {
			timeseriesBucket = new TimeSeriesBucket(timestamp);
			bucketMap.put(tsBucket, timeseriesBucket);
		}
		timeseriesBucket.addDataPoint(timestamp, value);
	}

	public void addDataPoint(TimeUnit unit, long timestamp, double value) throws RejectException {
		int bucket = TimeUtils.getTimeBucket(unit, timestamp, TIME_BUCKET_CONSTANT);
		String tsBucket = Integer.toHexString(bucket);
		TimeSeriesBucket timeseriesBucket = bucketMap.get(tsBucket);
		if (timeseriesBucket == null) {
			timeseriesBucket = new TimeSeriesBucket(timestamp);
			bucketMap.put(tsBucket, timeseriesBucket);
		}
		timeseriesBucket.addDataPoint(timestamp, value);
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
	public static List<DataPoint> seriesToDataPoints(List<DataPoint> points, TimeSeriesBucket timeSeries,
			Predicate timePredicate, Predicate valuePredicate, boolean isFp) {
		Reader reader = timeSeries.getReader(timePredicate, valuePredicate);
		DataPoint point = null;
		while (true) {
			try {
				point = reader.readPair();
				if (point != null) {
					point.setFp(isFp);
					points.add(point);
				}
			} catch (IOException e) {
				break;
			}
		}
		return points;
	}

	/**
	 * @return the bucketMap
	 */
	public SortedMap<String, TimeSeriesBucket> getBucketMap() {
		return bucketMap;
	}

	/**
	 * @return the fp
	 */
	public boolean isFp() {
		return fp;
	}

}
