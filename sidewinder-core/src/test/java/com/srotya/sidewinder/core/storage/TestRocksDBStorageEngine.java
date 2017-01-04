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
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

/**
 * @author ambudsharma
 *
 */
public class TestRocksDBStorageEngine implements Callback {

	@Test
	public void testBaseTimeSeriesWrites() throws Exception {
		// AbstractStorageEngine engine = new RocksDBStorageEngine();
		GorillaStorageEngine engine = new GorillaStorageEngine();
		engine.configure(new HashMap<>());
		engine.connect();

		long ts1 = System.currentTimeMillis();

		ExecutorService es = Executors.newCachedThreadPool();

		for (int k = 0; k < 50000; k++) {
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
		// try {
		// System.out.println("Series");
		// for (String string : engine.getSeries()) {
		// System.out.println("Series:"+string);
		// }
		// } catch (Exception e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
	}

	@Override
	public void complete() {
	}

}
