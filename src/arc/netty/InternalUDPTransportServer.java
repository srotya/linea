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
import java.net.NetworkInterface;
import java.util.List;

import com.srotya.linea.Event;
import com.srotya.linea.network.IWCHandler;
import com.srotya.linea.network.KryoObjectDecoder;
import com.srotya.linea.network.KryoObjectEncoder;
import com.srotya.linea.network.Router;
import com.srotya.linea.utils.NetworkUtils;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;

/**
 * @author ambud
 */
public class InternalUDPTransportServer {

	private Channel channel;
	private Router router;
	private int port;

	public InternalUDPTransportServer(Router router, int port) {
		this.router = router;
		this.port = port;
	}

	public void start() throws Exception {
		NetworkInterface iface = NetworkUtils.selectDefaultIPAddress(false);
		Inet4Address address = NetworkUtils.getIPv4Address(iface);
		EventLoopGroup workerGroup = new NioEventLoopGroup(4);

		Bootstrap b = new Bootstrap();
		channel = b.group(workerGroup).channel(NioDatagramChannel.class).option(ChannelOption.SO_RCVBUF, 10485760)
				.handler(new ChannelInitializer<Channel>() {

					@Override
					protected void initChannel(Channel ch) throws Exception {
						ch.pipeline().addLast(new KryoDatagramDecoderWrapper()).addLast(new IWCHandler(router));
					}
				}).bind(address, port).sync().channel();
	}

	public void stop() throws InterruptedException {
		channel.close().await();
	}

	public static class KryoDatagramDecoderWrapper extends MessageToMessageDecoder<DatagramPacket> {

		@Override
		protected void decode(ChannelHandlerContext ctx, DatagramPacket msg, List<Object> out) throws Exception {
			ByteBuf buf = msg.content();
			out.addAll(KryoObjectDecoder.bytebufToEvents(buf));
		}

	}

	public static class KryoDatagramEncoderWrapper extends MessageToMessageEncoder<Event> {

		@Override
		protected void encode(ChannelHandlerContext ctx, Event msg, List<Object> out) throws Exception {
			out.add(KryoObjectEncoder.eventToByteArray(msg));
		}

	}

}
