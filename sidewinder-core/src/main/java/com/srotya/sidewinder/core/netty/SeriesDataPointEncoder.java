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
package com.srotya.sidewinder.core.netty;

import java.util.List;

import com.srotya.sidewinder.core.storage.DataPoint;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * @author ambud
 */
public class SeriesDataPointEncoder extends MessageToByteEncoder<List<DataPoint>> {

	@Override
	protected void encode(ChannelHandlerContext arg0, List<DataPoint> dataPoints, ByteBuf buf) throws Exception {
		int size = dataPoints.size();
		buf.writeInt(size);
		for (DataPoint dataPoint : dataPoints) {
			encodeDPointToBuf(buf, dataPoint);
		}
	}

	public static void encodeDPointToBuf(ByteBuf buf, DataPoint dataPoint) {
		byte[] bytes = dataPoint.getSeriesName().getBytes();
		buf.writeInt(bytes.length);
		buf.writeBytes(bytes);
		buf.writeLong(dataPoint.getTimestamp());
		if (dataPoint.isFp()) {
			buf.writeByte('0');
			buf.writeDouble(dataPoint.getValue());
		}else {
			buf.writeByte('1');
			buf.writeLong(dataPoint.getLongValue());
		}
	}

}
