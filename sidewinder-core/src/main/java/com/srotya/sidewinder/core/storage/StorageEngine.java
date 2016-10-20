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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author ambudsharma
 */
public interface StorageEngine {
	
	/**
	 * @param conf
	 * @throws IOException
	 */
	public void configure(Map<String, String> conf) throws IOException;
	
	/**
	 * Connect to the storage engine
	 * @throws IOException
	 */
	public void connect() throws IOException;
	
	/**
	 * Disconnect from the storage engine
	 * @throws IOException
	 */
	public void disconnect()  throws IOException;
	
	/**
	 * @param seriesName
	 * @param tags
	 * @param unit
	 * @param timestamp
	 * @param value
	 * @throws IOException
	 */
	void writeSeries(String seriesName, List<String> tags, TimeUnit unit, long timestamp, long value) throws IOException;
	
	/**
	 * @param seriesName
	 * @param tags
	 * @param unit
	 * @param timestamp
	 * @param timeBucket
	 * @param value
	 * @throws IOException
	 */
	void writeSeries(String seriesName, List<String> tags,  TimeUnit unit, long timestamp, double value) throws IOException;

}
