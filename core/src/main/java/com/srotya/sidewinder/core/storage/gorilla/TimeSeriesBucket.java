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

import java.io.Serializable;
import java.nio.ByteBuffer;

import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.RejectException;

/**
 * In-memory representation of a time series based on Facebook's Gorilla compression. This
 * class wraps the compressed time series byte representation of Gorilla and
 * adds read-write concurrency and thread-safety using re-entrant locks.
 * 
 * @author ambud
 */
public class TimeSeriesBucket implements Serializable {

	private static final long serialVersionUID = 1L;
	private static final RejectException OLD_DATA_POINT = new RejectException("Rejected older datapoint");
	private Writer writer;
	private ByteBufferBitOutput output;
	private int count;
	private long lastTs;

	public TimeSeriesBucket(long headerTimestamp) {
		this.output = new ByteBufferBitOutput(4096 * 8 * 2);
		this.writer = new Writer(headerTimestamp, output);
	}

	public void addDataPoint(long timestamp, double value) throws RejectException {
		synchronized (output) {
			if (timestamp < lastTs) {
				// drop this datapoint
				throw OLD_DATA_POINT;
			}
			writer.addValue(timestamp, value);
			count++;
			lastTs = timestamp;
		}
	}

	public void addDataPoint(long timestamp, long value) throws RejectException {
		synchronized (output) {
			if (timestamp < lastTs) {
				// drop this datapoint
				throw OLD_DATA_POINT;
			}
			writer.addValue(timestamp, value);
			count++;
			lastTs = timestamp;
		}
	}

	public Reader getReader(Predicate timePredicate, Predicate valuePredicate) {
		ByteBuffer buf = null;
		int countSnapshot;
		synchronized (output) {
			buf = output.getByteBuffer().duplicate();
			countSnapshot = count;
		}
		buf.rewind();
		return new Reader(new ByteBufferBitInput(buf), countSnapshot, timePredicate, valuePredicate);
	}

	/**
	 * Flush byte to buffer
	 */
	public void flush() {
		synchronized (output) {
			writer.flush();
		}
	}

	/**
	 * Not threadsafe
	 * 
	 * @return the count
	 */
	public int getCount() {
		return count;
	}

	/**
	 * Analytical method used for monitoring compression ratios for a given
	 * timeseries. Ratio = expected number of bytes / actual number of bytes.
	 * 
	 * @return compression ratio
	 */
	public double getCompressionRatio() {
		synchronized (output) {
			ByteBuffer buf = output.getByteBuffer().duplicate();
			double expectedSize = count * 8 * 2;
			return expectedSize / buf.position();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "TimeSeries [count=" + count + "]";
	}
}