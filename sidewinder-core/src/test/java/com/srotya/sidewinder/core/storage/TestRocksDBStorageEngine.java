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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

/**
 * @author ambudsharma
 *
 */
public class TestRocksDBStorageEngine implements Callback {

	@Test
	public void testBaseTimeSeriesWrites() throws IOException, InterruptedException {
		AbstractStorageEngine engine = new RocksDBStorageEngine();
		engine.configure(new HashMap<>());
		engine.connect();
		
		AtomicInteger counter = new AtomicInteger(0);
		
		ScheduledExecutorService mon = Executors.newScheduledThreadPool(1);
		mon.scheduleAtFixedRate(()->{
			System.out.println("EPS:"+counter.getAndSet(0));
		}, 0, 1	, TimeUnit.SECONDS);
		
		ExecutorService es1 = Executors.newSingleThreadExecutor();
		es1.submit(engine);
		
		long timestamp = System.currentTimeMillis();
		ExecutorService es = Executors.newFixedThreadPool(4);
		
		for (int k = 0; k < 10; k++) {
			es.submit(() -> {
				Random r = new Random();
				for (int i = 0; i < 1000000; i++) {
					try {
						engine.writeSeries(r.nextInt(10000) + "testseries1223", Arrays.asList("cpu", "host1", "app1"),
								TimeUnit.MILLISECONDS, timestamp + (i * 1000 * 60), i, this);
						if (i % 1000 == 0) {
							counter.addAndGet(1000);
						}
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			});
		}

		es.shutdown();
		es.awaitTermination(1000, TimeUnit.SECONDS);
		engine.stop();
		es1.shutdown();
		mon.shutdown();
		engine.disconnect();
	}

	@Override
	public void complete() {
	}

}
