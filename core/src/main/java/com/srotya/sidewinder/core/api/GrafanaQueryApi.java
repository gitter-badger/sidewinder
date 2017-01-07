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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * @author ambud
 *
 */
@Path("/databases/{" + DatabaseOpsApi.DB_NAME + "}")
public class GrafanaQueryApi {

	private StorageEngine engine;

	public GrafanaQueryApi(StorageEngine engine) {
		this.engine = engine;
	}

	@Path("/hc")
	@GET
	public String getHealth(@PathParam(DatabaseOpsApi.DB_NAME) String dbName) {
		try {
			engine.checkIfExists(dbName);
			return "true";
		} catch (Exception e) {
			throw new NotFoundException("Database:" + dbName + " doesn't exist");
		}
	}

	@Path("/query")
	@POST
	@Produces({ MediaType.APPLICATION_JSON })
	@Consumes({ MediaType.APPLICATION_JSON })
	public List<Target> query(@PathParam(DatabaseOpsApi.DB_NAME) String dbName, String query) throws ParseException {
		// SimpleDateFormat sdf = new SimpleDateFormat();
		// sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		JsonObject json = gson.fromJson(query, JsonObject.class);
		// System.out.println(gson.toJson(json));
		JsonObject range = json.get("range").getAsJsonObject();
		LocalDateTime startTsT = LocalDateTime.parse(range.get("from").getAsString(),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
		ZoneId utc = ZoneId.of("UTC");
		long startTs = startTsT.atZone(utc).toInstant().toEpochMilli();
		// 1483664565096
		// 1483722162452
		LocalDateTime endTsT = LocalDateTime.parse(range.get("to").getAsString(),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
		long endTs = endTsT.atZone(utc).toInstant().toEpochMilli();

		System.out.println("From:" + startTs + "\t" + new Date(endTs) + "\t" + range.get("to").getAsString());
		List<String> measurementNames = new ArrayList<>();
		JsonArray targets = json.get("targets").getAsJsonArray();
		for (int i = 0; i < targets.size(); i++) {
			JsonElement jsonElement = targets.get(i).getAsJsonObject().get("target");
			if (jsonElement != null) {
				measurementNames.add(jsonElement.getAsString());
			}
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
		for (String measurementName : measurementNames) {
			Target tar = new Target(measurementName);
			// System.err.println(dbName + "\t" + measurementName);
			List<DataPoint> points = engine.queryDataPoints(dbName, measurementName, startTs, endTs, null);
			for (DataPoint dataPoint : points) {
				if (!dataPoint.isFp()) {
					tar.getDatapoints().add(new Number[] { dataPoint.getLongValue(), dataPoint.getTimestamp() });
				} else {
					tar.getDatapoints().add(new Number[] { dataPoint.getValue(), dataPoint.getTimestamp() });
				}
			}
			output.add(tar);
			System.err.println("Output:" + output + "\t" + points);
		}
		return output;
	}

	@Path("/query/search")
	@POST
	@Produces({ MediaType.APPLICATION_JSON })
	@Consumes({ MediaType.APPLICATION_JSON })
	public Set<String> querySeriesNames(@PathParam(DatabaseOpsApi.DB_NAME) String dbName, String queryString) {
		try {
			return engine.getMeasurementsLike(dbName, "");
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

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "Target [target=" + target + ", datapoints=" + datapoints + "]";
		}

	}

}
