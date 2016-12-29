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
package com.srotya.linea.network.netty;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.lmax.disruptor.EventHandler;
import com.srotya.linea.Event;
import com.srotya.linea.UnifiedFactory;
import com.srotya.linea.clustering.Columbus;
import com.srotya.linea.clustering.WorkerEntry;
import com.srotya.linea.network.KryoObjectEncoder;
import com.srotya.linea.utils.Constants;

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
 *
 */
public class InternalTCPTransportClient implements EventHandler<Event> {

	// private static final Logger logger =
	// Logger.getLogger(InternalUDPTransportClient.class.getName());
	private Map<Integer, Channel> socketMap;
	private Columbus columbus;

	public InternalTCPTransportClient(Columbus columbus) {
		this.columbus = columbus;
		socketMap = new HashMap<>();
	}

	public void start() throws Exception {
		for (Entry<Integer, WorkerEntry> entry : columbus.getWorkerMap().entrySet()) {
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
								p.addLast(new KryoObjectEncoder());
							}
						});

				// Start the client.
				ChannelFuture f = b.connect(entry.getValue().getWorkerAddress(), entry.getValue().getDataPort()).sync();
				socketMap.put(entry.getKey(), f.channel());
			} finally {
				// Shut down the event loop to terminate all threads.
			}
		}
		// workerGroup.shutdownGracefully().sync();
	}

	public void stop() throws InterruptedException {
		for (Entry<Integer, Channel> entry : socketMap.entrySet()) {
			entry.getValue().flush();
			entry.getValue().closeFuture().await();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lmax.disruptor.EventHandler#onEvent(java.lang.Object, long,
	 * boolean)
	 */
	@Override
	public void onEvent(Event event, long sequence, boolean endOfBatch) throws Exception {
		Integer workerId = (Integer) event.getHeaders().get(Constants.FIELD_DESTINATION_WORKER_ID);
		try {
			Channel socket = socketMap.get(workerId);
			byte[] ary = KryoObjectEncoder.eventToByteArray(event);
			socket.writeAndFlush(ary);
		} catch (Exception e) {
			System.out.println("Exception:" + event + "\tSelf worker ID:" + columbus.getSelfWorkerId() + "\t" + workerId
					+ "\t" + columbus.getWorkerMap());
			throw e;
		}
	}

	public static void main(String[] args) throws Exception {
		Columbus columbus = new Columbus(new HashMap<>());
		columbus.addKnownPeer(0,
				new WorkerEntry(InetAddress.getByName("localhost"), 12552, System.currentTimeMillis()));
		InternalTCPTransportClient client = new InternalTCPTransportClient(columbus);
		client.start();

		UnifiedFactory factory = new UnifiedFactory();
		Event event = factory.buildEvent();
		event.getHeaders().put("host", 1);
		event.getHeaders().put(Constants.FIELD_DESTINATION_WORKER_ID, 0);
		for (int i = 0; i < 100; i++)
			client.onEvent(event, 1, false);
		client.stop();
	}

}
