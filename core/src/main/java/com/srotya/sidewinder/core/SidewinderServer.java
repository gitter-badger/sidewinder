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
package com.srotya.sidewinder.core;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.srotya.sidewinder.core.api.GrafanaQueryApi;
import com.srotya.sidewinder.core.storage.GorillaStorageEngine;
import com.srotya.sidewinder.core.storage.StorageEngine;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

/**
 * @author ambud
 *
 */
public class SidewinderServer extends Application<SidewinderConfig> {

	private StorageEngine storageEngine;
	private static SidewinderServer sidewinderServer;

	@Override
	public void run(SidewinderConfig config, Environment env) throws Exception {
		storageEngine = new GorillaStorageEngine();
		storageEngine.configure(new HashMap<>());
		env.jersey().register(new GrafanaQueryApi(storageEngine));
		Random rand = new Random();
		LocalDateTime ldt = LocalDateTime.now();
		ZoneId utc = ZoneId.of("UTC");
		long ts = ldt.atZone(utc).toInstant().toEpochMilli();
		for (int i = 0; i < 10; i++) {
			System.out.println(
					"Input time:" + (System.currentTimeMillis() - (ts - (i * 60000))) + "\t" + (ts - (i * 60000)));
			storageEngine.writeSeries("test", "cpu", new ArrayList<>(), TimeUnit.MILLISECONDS, ts - (i * 60000),
					rand.nextInt() * 100, null);
		}
	}

	/**
	 * Main method to launch dropwizard app
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		sidewinderServer = new SidewinderServer();
		sidewinderServer.run(args);
	}

	/**
	 * @return
	 */
	public static SidewinderServer getSidewinderServer() {
		return sidewinderServer;
	}

	/**
	 * @return
	 */
	public StorageEngine getStorageEngine() {
		return storageEngine;
	}

}
