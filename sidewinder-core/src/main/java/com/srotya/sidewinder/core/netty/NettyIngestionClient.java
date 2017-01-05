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

import java.util.ArrayList;
import java.util.List;

import com.srotya.sidewinder.core.storage.DataPoint;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;

public class NettyIngestionClient {

	public static void main(String[] args) throws InterruptedException {
		EventLoopGroup group = new NioEventLoopGroup(2);
		try {
			Bootstrap b = new Bootstrap();
			b.group(group).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true)
					.option(ChannelOption.SO_RCVBUF, 10485760).option(ChannelOption.SO_SNDBUF, 10485760)
					.handler(new ChannelInitializer<SocketChannel>() {
						@Override
						public void initChannel(SocketChannel ch) throws Exception {
							ChannelPipeline p = ch.pipeline();
							p.addLast(new LengthFieldPrepender(4));
							p.addLast(new SeriesDataPointEncoder());
						}
					});

			// Start the client.
			ChannelFuture f = b.connect("localhost", 9927).sync();
			Channel channel = f.channel();
			List<DataPoint> data = new ArrayList<>();
			for (int i = 0; i < 42; i++) {
				DataPoint dp = new DataPoint("test", System.currentTimeMillis() + i, System.currentTimeMillis() + i);
				dp.setFp(false);
				data.add(dp);
			}
			channel.writeAndFlush(data);
			channel.closeFuture().await();
			System.exit(0);
		} finally {
			// Shut down the event loop to terminate all threads.
		}
	}

}
