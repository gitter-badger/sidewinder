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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.srotya.sidewinder.core.utils.ByteUtils;

/**
 * @author ambudsharma
 */
public class TestAbstractStorageEngine {

	@Test
	public void testBasicBuildRowKey() throws IOException {
		AbstractStorageEngine engine = new TestStorageEngine();
		long timestamp = System.currentTimeMillis();
		byte[] rowKey = engine.buildRowKey("testSeries1", Arrays.asList("one", "five", "ten"),
				TimeUnit.MILLISECONDS, timestamp);
		byte[] timeStamp = new byte[4];
		System.arraycopy(rowKey, rowKey.length - 4, timeStamp, 0, 4);
		assertEquals((timestamp / 1000 / AbstractStorageEngine.BUCKET_SIZE) * AbstractStorageEngine.BUCKET_SIZE,
				ByteUtils.bytesToIntMSB(timeStamp));
		assertEquals((byte) 't', rowKey[0]);
		assertEquals((byte) 'f', rowKey[3]);
		assertEquals((byte) 'o', rowKey[6]);
		assertEquals((byte) 't', rowKey[9]);
	}

	@Test
	public void testPerfBuildRowKey() throws IOException {
		AbstractStorageEngine engine = new TestStorageEngine();
		long timestamp = System.currentTimeMillis();
		for (int i = 0; i < 10000000; i++) {
			engine.buildRowKey("testSeries1", Arrays.asList("one", "five", "ten", "sixty", "eighty", "hundred"), TimeUnit.MILLISECONDS, timestamp);
		}
	}

	private class TestStorageEngine extends AbstractStorageEngine {

		@Override
		public void configure(Map<String, String> conf) throws IOException {
		}

		@Override
		public byte[] indexIdentifier(String identifier) throws IOException {
			return new byte[] { (byte) identifier.charAt(0), (byte) identifier.charAt(1), (byte) identifier.charAt(2) };
		}

		@Override
		public void writeSeriesPoint(byte[] rowKey, long tsBucketOffset, byte[] value) throws IOException {
			// do nothing
		}

		@Override
		public void connect() throws IOException {
		}

		@Override
		public void disconnect() throws IOException {
		}

	}

}
