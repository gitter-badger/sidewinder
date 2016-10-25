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
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

/**
 * @author ambudsharma
 *
 */
public class TestRocksDBStorageEngine {

	@Test
	public void testBaseTimeSeriesWrites() throws IOException, InterruptedException {
		AbstractStorageEngine engine = new RocksDBStorageEngine();
		engine.configure(new HashMap<>());
		engine.connect();
		long timestamp = System.currentTimeMillis();
		ExecutorService es = Executors.newFixedThreadPool(4);
		for (int k = 0; k < 10; k++) {
			es.submit(() -> {
				Random r = new Random();
				for (int i = 0; i < 1000000; i++) {
					try {
						engine.writeSeries(r.nextInt(100) + "testseries1223", Arrays.asList("cpu", "host1", "app1"),
								TimeUnit.MILLISECONDS, timestamp + (i*1000*60), i);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			});
		}

		es.shutdown();
		es.awaitTermination(1000, TimeUnit.SECONDS);
		engine.disconnect();
	}

}
