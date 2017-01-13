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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.RejectException;

/**
 * Unit tests for {@link TimeSeriesBucket}
 * 
 * @author ambud
 */
public class TestTimeSeriesBucket {

	@Test
	public void testReadWriteLongs() throws RejectException {
		long ts = System.currentTimeMillis();
		int count = 10000;
		TimeSeriesBucket series = new TimeSeriesBucket(ts);
		for (int i = 0; i < count; i++) {
			series.addDataPoint(ts + (i * 1000), i);
		}
		Reader reader = series.getReader(null, null);
		assertEquals(count, series.getCount());
		// longs have an issue with serialization and flushing on last value for
		// a given series
		series.flush();
		for (int i = 0; i < count; i++) {
			try {
				DataPoint pair = reader.readPair();
				assertEquals(ts + (i * 1000), pair.getTimestamp());
				assertEquals(i, pair.getLongValue());
			} catch (Exception e) {
				fail("Must not through EOS exception since the loop should stop at the last element");
			}
		}
	}

	@Test
	public void testReadWriteDoubles() throws RejectException {
		long ts = System.currentTimeMillis();
		int count = 1000;
		TimeSeriesBucket series = new TimeSeriesBucket(ts);
		for (int i = 0; i < 1000; i++) {
			series.addDataPoint(ts + (i * 1000), i * 1.2);
		}
		Reader reader = series.getReader(null, null);
		assertEquals(count, series.getCount());
		for (int i = 0; i < count; i++) {
			try {
				DataPoint pair = reader.readPair();
				assertEquals(ts + (i * 1000), pair.getTimestamp());
				assertEquals(i * 1.2, pair.getValue(), 0.01);
			} catch (Exception e) {
				fail("Must not through EOS exception since the loop should stop at the last element");
			}
		}
	}

	@Test
	public void testCompressionRatios() throws RejectException {
		long ts = System.currentTimeMillis();
		TimeSeriesBucket series = new TimeSeriesBucket(ts);
		for (int i = 0; i < 10000; i++) {
			series.addDataPoint(ts + (i * 1000), i);
		}
		System.out.println("Test compression ratio (10K longs 1s frequency):" + series.getCompressionRatio());

		series = new TimeSeriesBucket(ts);
		for (int i = 0; i < 10000; i++) {
			series.addDataPoint(ts + i, i);
		}
		System.out.println("Test compression ratio (10K longs 1ms frequency):" + series.getCompressionRatio());

		series = new TimeSeriesBucket(ts);
		for (int i = 0; i < 10000; i++) {
			series.addDataPoint(ts + (i * 1000), i * 1.2);
		}
		System.out.println("Test compression ratio (10K double 1s frequency):" + series.getCompressionRatio());

		series = new TimeSeriesBucket(ts);
		Random rand = new Random();
		for (int i = 0; i < 10000; i++) {
			series.addDataPoint(ts + (i * 1000), rand.nextLong());
		}
		System.out.println("Test compression ratio (10K random 1s frequency):" + series.getCompressionRatio());
	}

	@Test
	public void testConcurrentReadWrites() throws RejectException {
		final long ts = System.currentTimeMillis();
		final TimeSeriesBucket series = new TimeSeriesBucket(ts);
		final AtomicBoolean startFlag = new AtomicBoolean(false);
		ExecutorService es = Executors.newCachedThreadPool();
		for (int i = 0; i < 2; i++) {
			es.submit(() -> {
				while (!startFlag.get()) {
					try {
						Thread.sleep(1);
					} catch (InterruptedException e) {
						return;
					}
				}
				for (int k = 0; k < 10; k++) {
					Reader reader = series.getReader(null, null);
					try {
						int c = 0;
						while (true) {
							reader.readPair();
							c = c + 1;
						}
					} catch (IOException e) {
					}
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						return;
					}
				}
			});
		}

		startFlag.set(true);
		for (int i = 0; i < 20; i++) {
			series.addDataPoint(ts + i, i * 1.2);
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				break;
			}
		}

		es.shutdownNow();

		Reader reader = series.getReader(null, null);
		try {
			for (int i = 0; i < 20; i++) {
				DataPoint pair = reader.readPair();
				assertEquals(ts + i, pair.getTimestamp());
				assertEquals(i * 1.2, pair.getValue(), 0.01);
			}
		} catch (IOException e) {
		}
	}

}
