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

import java.util.concurrent.TimeUnit;

/**
 * Utility class offering time functions
 * 
 * @author ambudsharma
 */
public class TimeUtils {

	private static final IllegalArgumentException ARGUMENT_EXCEPTION = new IllegalArgumentException();

	private TimeUtils() {
	}

	/**
	 * Floor long time to supplied Window
	 * 
	 * @param timeInMilliSeconds
	 * @param windowSizeInSeconds
	 * @return windowedTime
	 */
	public static int getWindowFlooredTime(long time, int windowSizeInSeconds, TimeUnit unit)
			throws IllegalArgumentException {
		int ts;
		ts = timeToSeconds(time, unit);
		return getWindowFlooredTime(ts, windowSizeInSeconds);
	}

	/**
	 * Convert supplied timestamp to seconds;
	 * 
	 * @param time
	 * @param unit
	 * @return
	 */
	public static int timeToSeconds(long time, TimeUnit unit) {
		int ts;
		switch (unit) {
		case NANOSECONDS:
			ts = (int) (time / (1000 * 1000 * 1000));
			break;
		case MICROSECONDS:
			ts = (int) (time / (1000 * 1000));
			break;
		case MILLISECONDS:
			ts = (int) (time / 1000);
			break;
		case SECONDS:
			ts = (int) time;
			break;
		default:
			throw ARGUMENT_EXCEPTION;
		}
		return ts;
	}

	/**
	 * Floor integer time to supplied Window
	 * 
	 * @param timeInSeconds
	 * @param windowSizeInSeconds
	 * @return windowedTime
	 */
	public static int getWindowFlooredTime(int timeInSeconds, int windowSizeInSeconds) {
		return timeInSeconds = ((timeInSeconds / windowSizeInSeconds) * windowSizeInSeconds);
	}

	/**
	 * Get integer time offset from the supplied Window
	 * 
	 * @param time
	 * @param windowSizeInSeconds
	 * @return offset
	 */
	public static short getWindowOffsetTime(long time, int windowSizeInSeconds, TimeUnit unit) {
		int ts = timeToSeconds(time, unit);
		return getWindowOffsetTime(ts, windowSizeInSeconds);
	}

	/**
	 * Get integer time offset from the supplied Window
	 * 
	 * @param time
	 * @param windowSizeInSeconds
	 * @return offset
	 */
	public static short getWindowOffsetTime(int time, int windowSizeInSeconds) {
		return (short) (time % windowSizeInSeconds);
	}

}
