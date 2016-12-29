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

import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.lmax.disruptor.EventHandler;
import com.srotya.linea.Event;
import com.srotya.linea.MutableShort;
import com.srotya.linea.clustering.Columbus;
import com.srotya.linea.network.KryoObjectEncoder;
import com.srotya.linea.utils.Constants;
import com.srotya.linea.utils.NetworkUtils;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;

/**
 * @author ambud
 *
 */
public class InternalUDPTransportClient implements EventHandler<Event> {

	// private static final Logger logger =
	// Logger.getLogger(InternalUDPTransportClient.class.getName());
	private Channel channel;
	private Map<Integer, ByteBuffer> bufferMap;
	private Map<Integer, MutableShort> bufferCounterMap;
	private Columbus columbus;
	private int port;
	private boolean packingEnabled;

	public InternalUDPTransportClient(Columbus columbus, int port, boolean packingEnabled) {
		this.columbus = columbus;
		this.port = port;
		System.err.println("UDP Client Port:" + port);
		this.packingEnabled = packingEnabled;
		int selfId = columbus.getSelfWorkerId();
		bufferMap = new HashMap<>();
		bufferCounterMap = new HashMap<>();
		for (int i = 0; i < columbus.getWorkerCount(); i++) {
			if (i != selfId) {
				ByteBuffer buf = ByteBuffer.allocate(800);
				buf.putShort((short) 0);
				bufferMap.put(i, buf);
				bufferCounterMap.put(i, new MutableShort((short) 0));
			}
		}
	}

	public void init() throws Exception {
		NetworkInterface iface = NetworkUtils.selectDefaultIPAddress(false);
		Inet4Address address = NetworkUtils.getIPv4Address(iface);
		// logger.info("Selected default interface:" + iface.getName() + "\twith
		// address:" + address.getHostAddress());

		EventLoopGroup workerGroup = new NioEventLoopGroup(1);
		Bootstrap b = new Bootstrap();
		channel = b.group(workerGroup).channel(NioDatagramChannel.class).option(ChannelOption.SO_SNDBUF, 10485760)
				.handler(new ChannelInitializer<Channel>() {

					@Override
					protected void initChannel(Channel ch) throws Exception {
						ch.pipeline();
					}
				}).bind(address, port).sync().channel();

		// workerGroup.shutdownGracefully().sync();
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
			ByteBuffer buf = bufferMap.get(workerId);
			MutableShort bufferEventCount = bufferCounterMap.get(workerId);
			if (event.getOriginEventId() == null) {
				System.err.println("Event originid null");
			}
			byte[] bytes = KryoObjectEncoder.eventToByteArray(event);
			if (packingEnabled) {
				if (bytes.length > 1024) {
					// discard
					System.err.println("Discarded event");
				}
				if (buf.remaining() - bytes.length >= 0 && bufferEventCount.getVal()<4) {
					buf.put(bytes);
					bufferEventCount.incrementAndGet();
				} else {
					buf.putShort(0, bufferEventCount.getVal());
					buf.rewind();
					channel.writeAndFlush(new DatagramPacket(Unpooled.wrappedBuffer(buf),
							new InetSocketAddress(columbus.getWorkerMap().get(workerId).getWorkerAddress(),
									columbus.getWorkerMap().get(workerId).getDataPort())))
							.await();
					bufferEventCount.setVal((short) 0);
					buf.rewind();
					buf.putShort(bufferEventCount.getVal());
					buf.put(bytes);
				}
			} else {
				buf.rewind();
				buf.putShort((short) 1);
				buf.put(bytes);
				buf.rewind();
				channel.writeAndFlush(new DatagramPacket(Unpooled.copiedBuffer(buf),
						new InetSocketAddress(columbus.getWorkerMap().get(workerId).getWorkerAddress(),
								columbus.getWorkerMap().get(workerId).getDataPort())));
			}

		} catch (Exception e) {
			System.out.println("Exception:" + event + "\tSelf worker ID:" + columbus.getSelfWorkerId() + "\t" + workerId
					+ "\t" + columbus.getWorkerMap());
			throw e;
		}
	}

}
