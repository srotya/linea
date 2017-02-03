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
package com.srotya.linea.network.netty;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import com.srotya.linea.Tuple;
import com.srotya.linea.clustering.WorkerEntry;
import com.srotya.linea.network.KryoObjectEncoder;
import com.srotya.linea.network.NetworkClient;

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

/**
 * @author ambud
 */
public class NettyClient<E extends Tuple> extends NetworkClient<E> {

	private Map<Integer, Channel> channelMap;
	private Logger logger;

	public NettyClient() {
		this.logger = Logger.getLogger(NettyClient.class.getName());
		channelMap = new HashMap<>();
	}

	public void start() throws Exception {
		for (Entry<Integer, WorkerEntry> entry : getColumbus().getWorkerMap().entrySet()) {
			if (entry.getKey() != getColumbus().getSelfWorkerId()) {
				Integer key = entry.getKey();
				WorkerEntry value = entry.getValue();
				retryConnectLoop(key, value);
			}
		}
	}

	protected void retryConnectLoop(Integer key, WorkerEntry value) throws InterruptedException, IOException {
		boolean connected = false;
		int retryCount = 1;
		while (!connected && retryCount < 100) {
			Channel channel = tryConnect(value, retryCount);
			if (channel != null) {
				connected = true;
				channelMap.put(key, channel);
			}
			retryCount++;
		}
	}

	protected Channel tryConnect(WorkerEntry value, int retryCount) throws InterruptedException {
		try {
			EventLoopGroup group = new NioEventLoopGroup(2);
			try {
				Bootstrap b = new Bootstrap();
				b.group(group).channel(NioSocketChannel.class).option(ChannelOption.SO_KEEPALIVE, true)
						.option(ChannelOption.SO_RCVBUF, 10485760).option(ChannelOption.SO_SNDBUF, 10485760)
						.handler(new ChannelInitializer<SocketChannel>() {
							@Override
							public void initChannel(SocketChannel ch) throws Exception {
								ChannelPipeline p = ch.pipeline();
								p.addLast(new LengthFieldPrepender(2));
							}
						});

				// Start the client.
				ChannelFuture f = b.connect(value.getWorkerAddress(), value.getDataPort()).sync();
				return f.channel();
			} finally {
				// Shut down the event loop to terminate all threads.
				group.shutdownGracefully();
			}
		} catch (Exception e) {
			logger.warning("Worker connection refused:" + value.getWorkerAddress() + ". Retrying in " + retryCount
					+ " seconds.....");
			Thread.sleep(1000 * retryCount);
		}
		return null;
	}

	@Override
	public void onEvent(E event, long sequence, boolean endOfBatch) throws Exception {
		int workerId = event.getDestinationWorkerId();
		try {
			if (workerId % getClientThreads() == getClientThreadId()) {
				Channel channel = channelMap.get(workerId);
				byte[] ary = KryoObjectEncoder.eventToByteArray(event);
				channel.write(ary);
				if (endOfBatch) {
					channel.flush();
				}
			}
		} catch (Exception e) {
			System.out.println("Exception:" + event + "\tSelf worker ID:" + getColumbus().getSelfWorkerId() + "\t"
					+ workerId + "\t" + getColumbus().getWorkerMap());
			throw e;
		}
	}

}
