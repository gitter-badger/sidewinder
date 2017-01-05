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
package com.srotya.sidewinder.core.storage;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.srotya.sidewinder.core.utils.TimeUtils;
import com.srotya.sidewinder.gorillac.ByteBufferBitInput;
import com.srotya.sidewinder.gorillac.ByteBufferBitOutput;
import com.srotya.sidewinder.gorillac.Reader;
import com.srotya.sidewinder.gorillac.Writer;

/**
 * @author ambud
 */
public class GorillaStorageEngine implements StorageEngine {

	private SortedMap<String, TimeSeries> seriesMap;
	private AtomicInteger counter = new AtomicInteger(0);

	@Override
	public void configure(Map<String, String> conf) throws IOException {
		seriesMap = new ConcurrentSkipListMap<>();
		Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
			System.out.println(counter.getAndSet(0));
		}, 0, 1, TimeUnit.SECONDS);
	}

	@Override
	public void writeSeries(String dbName, String seriesName, List<String> tags, TimeUnit unit, long timestamp,
			long value, Callback callback) throws IOException {
		TimeSeries timeSeries = getOrCreateTimeSeries(dbName, seriesName, tags, unit, timestamp);
		timeSeries.addDatapoint(timestamp, value);
		counter.incrementAndGet();
	}

	protected TimeSeries getOrCreateTimeSeries(String dbName, String seriesName, List<String> tags, TimeUnit unit,
			long timestamp) {
		int bucket = TimeUtils.getTimeBucket(unit, timestamp, 4096);
		String tsBucket = Integer.toHexString(bucket);
		StringBuilder builder = new StringBuilder(seriesName.length() + 1 + tsBucket.length());
		builder.append(seriesName);
		builder.append("_");
		builder.append(tsBucket);
		String rowKey = builder.toString();
		TimeSeries timeSeries = seriesMap.get(rowKey);
		if (timeSeries == null) {
			timeSeries = new TimeSeries(timestamp);
			seriesMap.put(rowKey, timeSeries);
		}
		return timeSeries;
	}

	@Override
	public void writeSeries(String dbName, String seriesName, List<String> tags, TimeUnit unit, long timestamp,
			double value, Callback callback) throws IOException {
		TimeSeries timeSeries = getOrCreateTimeSeries(dbName, seriesName, tags, unit, timestamp);
		timeSeries.addDatapoint(timestamp, value);
		counter.incrementAndGet();
	}

	@Override
	public Set<String> getDatabases() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> getSeries(String dbName) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void deleteAllData() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean checkIfExists(String dbName) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void truncateDatabase(String dbName) throws Exception {
		// TODO Auto-generated method stub

	}

	/**
	 * In-memory representation of a time series.
	 * 
	 * @author ambud
	 */
	public static class TimeSeries implements Serializable {

		private static final long serialVersionUID = 1L;
		private Writer compressor;
		private ByteBufferBitOutput output;
		private int count;

		public TimeSeries(long headerTimestamp) {
			this.output = new ByteBufferBitOutput(4096 * 8);
			this.compressor = new Writer(headerTimestamp, output);
		}

		public void addDatapoint(long timestamp, double value) {
			synchronized (output) {
				compressor.addValue(timestamp, value);
			}
		}

		public void addDatapoint(long timestamp, long value) {
			synchronized (output) {
				compressor.addValue(timestamp, value);
			}
		}

		public Reader getReader() {
			ByteBuffer buf = null;
			int countSnapshot;
			synchronized (output) {
				buf = output.getByteBuffer().duplicate();
				countSnapshot = count;
			}
			buf.rewind();
			return new Reader(new ByteBufferBitInput(buf), countSnapshot);
		}
	}

	@Override
	public void connect() throws IOException {
	}

	@Override
	public void disconnect() throws IOException {
	}

}
