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
package com.srotya.sidewinder.core.api;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.ws.rs.Consumes;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * @author ambud
 *
 */
@Path("/query")
public class GrafanaQueryApi {

	private StorageEngine engine;

	public GrafanaQueryApi(StorageEngine engine) {
		this.engine = engine;
	}

	@POST
	@Produces({ MediaType.APPLICATION_JSON })
	@Consumes({ MediaType.APPLICATION_JSON })
	public List<Target> query(String query) {
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		JsonObject json = gson.fromJson(query, JsonObject.class);
		JsonObject range = json.get("range").getAsJsonObject();
		int interval = json.get("intervalMs").getAsInt();
		List<String> measurementNames = new ArrayList<>();
		JsonArray targets = json.get("targets").getAsJsonArray();
		for (int i = 0; i < targets.size(); i++) {
			measurementNames.add(targets.get(i).getAsJsonObject().get("target").getAsString());
		}
		/*
		 * { "panelId": 1, "range": { "from": "2017-01-06T01:39:12.670Z", "to":
		 * "2017-01-06T07:39:12.670Z", "raw": { "from": "now-6h", "to": "now" }
		 * }, "rangeRaw": { "from": "now-6h", "to": "now" }, "interval": "20s",
		 * "intervalMs": 20000, "targets": [ { "target": "testseries", "refId":
		 * "A", "type": "timeserie" } ], "format": "json", "maxDataPoints": 1039
		 * }
		 */
		List<Target> output = new ArrayList<>();

		Random rand = new Random();
		Target tar = new Target("testseries");
		for (int i = 0; i < 1000; i++) {
			tar.getDatapoints().add(new Number[] { rand.nextDouble()*100, (System.currentTimeMillis() - (20000 * i)) });
		}
		output.add(tar);
		return output;
	}

	@Path("/search")
	@POST
	@Produces({ MediaType.APPLICATION_JSON })
	public List<String> querySeriesNames(String seriesName) {
		try {
			return engine.getSeriesLike(seriesName);
		} catch (IOException e) {
			throw new InternalServerErrorException(e.getMessage());
		}
	}

	public static class Target implements Serializable {

		private static final long serialVersionUID = 1L;

		private String target;
		private List<Number[]> datapoints;

		public Target(String target) {
			this.target = target;
			datapoints = new ArrayList<>();
		}

		/**
		 * @return the target
		 */
		public String getTarget() {
			return target;
		}

		/**
		 * @param target
		 *            the target to set
		 */
		public void setTarget(String target) {
			this.target = target;
		}

		/**
		 * @return the datapoints
		 */
		public List<Number[]> getDatapoints() {
			return datapoints;
		}

		/**
		 * @param datapoints
		 *            the datapoints to set
		 */
		public void setDatapoints(List<Number[]> datapoints) {
			this.datapoints = datapoints;
		}

	}

}
