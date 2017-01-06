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
package com.srotya.sidewinder.core.storage;

import java.io.Serializable;
import java.util.List;

/**
 * @author ambud
 */
public class DataPoint implements Serializable {

	private static final long serialVersionUID = 1L;
	private boolean isFp;
	private String seriesName;
	private List<String> tags;
	private long timestamp;
	private long value;

	public DataPoint(long timestamp, long value) {
		this.timestamp = timestamp;
		this.value = value;
	}

	public DataPoint(String seriesName, List<String> tags, long timestamp, long value) {
		this.seriesName = seriesName;
		this.tags = tags;
		this.timestamp = timestamp;
		this.value = value;
	}

	public DataPoint(String seriesName, List<String> tags, long timestamp, double value) {
		this.seriesName = seriesName;
		this.tags = tags;
		this.timestamp = timestamp;
		this.value = Double.doubleToLongBits(value);
	}

	/**
	 * @return the seriesName
	 */
	public String getSeriesName() {
		return seriesName;
	}

	/**
	 * @param seriesName
	 *            the seriesName to set
	 */
	public void setSeriesName(String seriesName) {
		this.seriesName = seriesName;
	}

	/**
	 * @return the timestamp
	 */
	public long getTimestamp() {
		return timestamp;
	}

	/**
	 * @param timestamp
	 *            the timestamp to set
	 */
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	/**
	 * @return
	 */
	public long getLongValue() {
		return value;
	}

	/**
	 * @return the value
	 */
	public double getValue() {
		return Double.longBitsToDouble(value);
	}

	/**
	 * @param value
	 *            the value to set
	 */
	public void setValue(long value) {
		this.value = value;
	}

	/**
	 * @return the isFp
	 */
	public boolean isFp() {
		return isFp;
	}

	/**
	 * @param isFp the isFp to set
	 */
	public void setFp(boolean isFp) {
		this.isFp = isFp;
	}

	/**
	 * @return the tags
	 */
	public List<String> getTags() {
		return tags;
	}

	/**
	 * @param tags the tags to set
	 */
	public void setTags(List<String> tags) {
		this.tags = tags;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "DataPoint [seriesName=" + seriesName + ", timestamp=" + timestamp + ", value=" + getValue() + "]";
	}

}
