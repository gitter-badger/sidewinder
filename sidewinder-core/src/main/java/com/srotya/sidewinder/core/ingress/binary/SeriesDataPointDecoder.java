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
package com.srotya.sidewinder.core.ingress.binary;

import java.util.List;

import com.srotya.sidewinder.core.storage.DataPoint;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

/**
 * @author ambud
 */
public class SeriesDataPointDecoder extends ReplayingDecoder<Void> {

	@Override
	protected void decode(ChannelHandlerContext arg0, ByteBuf buf, List<Object> output) throws Exception {
		int dpCount = buf.readInt();
		for (int i = 0; i < dpCount; i++) {
			DataPoint d = decodeBufToDPoint(buf);
			if (d == null) {
				System.out.println("Bad data point");
				return;
			} else {
				output.add(d);
			}
		}
	}

	public static DataPoint decodeBufToDPoint(ByteBuf buf) {
		int seriesNameLength = buf.readInt();
		if(seriesNameLength<0) {
			return null;
		}
		byte[] b = new byte[seriesNameLength];
		buf.readBytes(b);
		String seriesName = new String(b);
		long timestamp = buf.readLong();
		byte flag = buf.readByte();
		DataPoint dp;
		if (flag == '0') {
			double value = buf.readDouble();
			dp = new DataPoint(seriesName, null, timestamp, value);
			dp.setFp(true);
		} else {
			long value = buf.readLong();
			dp = new DataPoint(seriesName, null, timestamp, value);
		}
		return dp;
	}

}
