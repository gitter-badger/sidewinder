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
	private TimeZone tz;

	public GrafanaQueryApi(StorageEngine engine) {
		this.engine = engine;
		tz = TimeZone.getDefault();
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
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		JsonObject json = gson.fromJson(query, JsonObject.class);
		System.err.println(gson.toJson(json));
		JsonObject range = json.get("range").getAsJsonObject();
		long startTs = sdf.parse(range.get("from").getAsString()).getTime();
		long endTs = sdf.parse(range.get("to").getAsString()).getTime();
		
		startTs = tz.getOffset(startTs)+startTs;
		endTs = tz.getOffset(endTs)+endTs;

		System.out.println("From:" + new Date(startTs) + "\tTo:" + new Date(endTs) + "\tRaw To:" + range);
		List<String> measurementNames = new ArrayList<>();
		JsonArray targets = json.get("targets").getAsJsonArray();
		for (int i = 0; i < targets.size(); i++) {
			JsonElement jsonElement = targets.get(i).getAsJsonObject().get("target");
			if (jsonElement != null) {
				measurementNames.add(jsonElement.getAsString());
			}
		}

		List<Target> output = new ArrayList<>();
		for (String measurementName : measurementNames) {
			List<DataPoint> points = engine.queryDataPoints(dbName, measurementName, startTs, endTs, null, null);
			Target tar = new Target(measurementName);
			for (DataPoint dataPoint : points) {
				if (!dataPoint.isFp()) {
					tar.getDatapoints().add(new Number[] { dataPoint.getLongValue(), dataPoint.getTimestamp() });
				} else {
					tar.getDatapoints().add(new Number[] { dataPoint.getValue(), dataPoint.getTimestamp() });
				}
			}
			output.add(tar);
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
