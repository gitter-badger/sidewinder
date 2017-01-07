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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

/**
 * @author ambud
 */
public class TestGorillaStorageEngine implements Callback {

	@Test
	public void testBaseTimeSeriesWrites() throws Exception {
		GorillaStorageEngine engine = new GorillaStorageEngine();
		engine.configure(new HashMap<>());
		engine.connect();

		long ts1 = System.currentTimeMillis();

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
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			});
		}
		es.shutdown();
		es.awaitTermination(10, TimeUnit.SECONDS);

		System.out.println((System.currentTimeMillis() - ts1) + "\tms");
	}

	@Test
	public void testReads() throws IOException {
		StorageEngine engine = new GorillaStorageEngine();
		engine.configure(new HashMap<>());
		long ts = System.currentTimeMillis();
		engine.writeSeries("test", "cpu", null, TimeUnit.MILLISECONDS, ts, 1, null);
		engine.writeSeries("test", "cpu", null, TimeUnit.MILLISECONDS, ts + (400 * 60000), 4, null);
		List<DataPoint> queryDataPoints = engine.queryDataPoints("test", "cpu", ts, ts + (400 * 60000), null);
		assertEquals(ts, queryDataPoints.get(0).getTimestamp());
	}

	@Override
	public void complete() {
	}

}
