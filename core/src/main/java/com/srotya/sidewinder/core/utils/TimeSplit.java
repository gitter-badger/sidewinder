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
package com.srotya.sidewinder.core.utils;

import java.io.Serializable;

public class TimeSplit implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private int bucket;
	private long offset;
	
	public TimeSplit() {
	}

	/**
	 * @return the bucket
	 */
	public int getBucket() {
		return bucket;
	}

	/**
	 * @param bucket the bucket to set
	 */
	public void setBucket(int bucket) {
		this.bucket = bucket;
	}

	/**
	 * @return the offset
	 */
	public long getOffset() {
		return offset;
	}

	/**
	 * @param offset the offset to set
	 */
	public void setOffset(long offset) {
		this.offset = offset;
	}

}
