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

import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeMap;
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
		AbstractStorageEngine engine = new RocksDBStorageEngine();
		engine.configure(new HashMap<>());
		engine.connect();
		
		long timestamp = System.currentTimeMillis();
		ExecutorService es = Executors.newCachedThreadPool();
		es.submit(engine);
		engine.writeSeries(10 + "testseries1223", Arrays.asList("cpu", "host1", "app1"),
				TimeUnit.MILLISECONDS, timestamp + (2 * 1000 * 60), 10, this);

		byte[] rowKey = engine.buildRowKey(10 + "testseries1223", Arrays.asList("cpu", "host1", "app1"),
				TimeUnit.MILLISECONDS, timestamp + (2 * 1000 * 60));
		engine.stop();
		es.shutdown();
		es.awaitTermination(1000, TimeUnit.SECONDS);
		TreeMap<Long, byte[]> values = engine.getTreeFromDS(rowKey);
		System.out.println("Values:"+values);
		engine.print();
		engine.getSeries().forEach(series->System.out.println(series));
		engine.disconnect();
//		try {
//			System.out.println("Series");
//			for (String string : engine.getSeries()) {
//				System.out.println("Series:"+string);
//			}
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}

	@Override
	public void complete() {
	}

}
