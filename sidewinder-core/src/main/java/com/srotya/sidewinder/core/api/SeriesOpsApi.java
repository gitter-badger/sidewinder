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
package com.srotya.sidewinder.core.api;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * @author ambud
 */
@Path("/database/{dbName}/series/{" + SeriesOpsApi.SERIES_NAME + "}")
public class SeriesOpsApi {

	public static final String SERIES_NAME = "seriesName";
	private StorageEngine storageEngine;

	public SeriesOpsApi(StorageEngine storageEngine) {
		this.storageEngine = storageEngine;
	}

	@Path("/")
	@DELETE
	public void dropSeries(@PathParam(DatabaseOpsApi.DB_NAME) String dbName,
			@PathParam(SERIES_NAME) String seriesName) {

	}

	@Path("/check")
	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	public void checkSeries(@PathParam(DatabaseOpsApi.DB_NAME) String dbName,
			@PathParam(SERIES_NAME) String seriesName) {

	}

	@Path("/")
	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	public List<SortedMap<String, Object>> getSeries(@PathParam(DatabaseOpsApi.DB_NAME) String dbName,
			@PathParam(SERIES_NAME) String seriesName, @QueryParam("startTime") String startTime,
			@QueryParam("endTime") String endTime) {
		try {
			List<SortedMap<String, Object>> seriesData = null;
			if (seriesData.isEmpty()) {
				throw new NotFoundException("Database/Series not found");
			} else {
				return seriesData;
			}
		} catch (NotFoundException e) {
			throw e;
		} catch (Exception e) {
			throw new InternalServerErrorException(e);
		}
	}

}
