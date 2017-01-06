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
package com.srotya.sidewinder.core.ingress.http;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.StorageEngine;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;

/**
 * HTTP Protocol follows an InfluxDB wire format for ease of use with clients.
 * 
 * References:
 * https://netty.io/4.0/xref/io/netty/example/http/snoop/HttpSnoopServerHandler.html
 * 
 * @author ambud
 */
public class HTTPDataPointDecoder extends SimpleChannelInboundHandler<Object> {

	private StringBuilder responseString = new StringBuilder();
	private HttpRequest request;
	private StorageEngine engine;

	public HTTPDataPointDecoder(StorageEngine engine) {
		this.engine = engine;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg instanceof HttpRequest) {
			HttpRequest request = this.request = (HttpRequest) msg;
			if (HttpUtil.is100ContinueExpected(request)) {
				send100Continue(ctx);
			}
		}

		if (msg instanceof HttpContent) {
			HttpContent httpContent = (HttpContent) msg;

			ByteBuf byteBuf = httpContent.content();
			if (byteBuf.isReadable()) {
				String payload = byteBuf.toString(CharsetUtil.UTF_8);
				List<DataPoint> dps = dataPointsFromString(payload);
				for (DataPoint dp : dps) {
					System.out.println("Datapoint:" + dp);
					try {
						engine.writeDataPoint(null, dp);
					} catch (IOException e) {
						responseString.append("Dropped:"+dp);
					}
				}
			}

			if (msg instanceof LastHttpContent) {
//				LastHttpContent lastHttpContent = (LastHttpContent) msg;
//				if (!lastHttpContent.trailingHeaders().isEmpty()) {
//				}
				if (writeResponse(request, ctx)) {
					ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
				}
			}
		}

	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		cause.printStackTrace();
		ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
	}

	public static List<DataPoint> dataPointsFromString(String payload) {
		List<DataPoint> dps = new ArrayList<>();
		String[] splits = payload.split("\n");
		for (String split : splits) {
			String[] parts = split.split("\\s+");
			if (parts.length < 2 || parts.length > 3) {
				// invalid datapoint => drop
				continue;
			}
			long timestamp = System.currentTimeMillis();
			if (parts.length == 3) {
				timestamp = Long.parseLong(parts[2]);
			}
			String[] key = parts[0].split(",");
			String seriesName = key[0];
			List<String> tags = new ArrayList<>();
			for (int i = 1; i < key.length; i++) {
				tags.add(key[i]);
			}
			String[] fields = parts[1].split(",");
			for (String field : fields) {
				String[] fv = field.split("=");
				String prefix = fv[0];
				if (fv[1].contains(".")) {
					double value = Double.parseDouble(fv[1]);
					DataPoint dp = new DataPoint(seriesName + "-" + prefix, tags, timestamp, value);
					dp.setFp(true);
					dps.add(dp);
				} else {
					long value = Long.parseLong(fv[1]);
					DataPoint dp = new DataPoint(seriesName + "-" + prefix, tags, timestamp, value);
					dp.setFp(false);
					dps.add(dp);
				}
			}
		}
		return dps;
	}

	private boolean writeResponse(HttpObject httpObject, ChannelHandlerContext ctx) {

		FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1,
				httpObject.decoderResult().isSuccess() ? OK : BAD_REQUEST, Unpooled.copiedBuffer(responseString.toString().toString(), CharsetUtil.UTF_8));
		response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");

		response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
		response.headers().set(CONNECTION, HttpHeaderValues.KEEP_ALIVE);

		// Write the response.
		ctx.write(response);
		return true;
	}

	private static void send100Continue(ChannelHandlerContext ctx) {
		FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, CONTINUE);
		ctx.write(response);
	}
}
