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

import com.esotericsoftware.kryo.Kryo;
import com.srotya.linea.network.IWCHandler;
import com.srotya.linea.network.KryoObjectDecoder;
import com.srotya.linea.network.Router;
import com.srotya.linea.utils.NetworkUtils;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

/**
 * 
 * 
 * @author ambud
 */
public class InternalTCPTransportServer {

	public static final ThreadLocal<Kryo> kryoThreadLocal = new ThreadLocal<Kryo>() {
		@Override
		protected Kryo initialValue() {
			Kryo kryo = new Kryo();
			return kryo;
		}
	};

	private Router router;
	private int port;
	private Channel channel;

	public InternalTCPTransportServer(Router router, int port) {
		this.router = router;
		this.port = port;
	}

	public void start() throws Exception {
		NetworkInterface iface = NetworkUtils.selectDefaultIPAddress(false);
		Inet4Address address = NetworkUtils.getIPv4Address(iface);

		EventLoopGroup bossGroup = new NioEventLoopGroup(1);
		EventLoopGroup workerGroup = new NioEventLoopGroup(3);

		ServerBootstrap b = new ServerBootstrap();
		channel = b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
				.option(ChannelOption.SO_RCVBUF, 10485760).option(ChannelOption.SO_SNDBUF, 10485760)
				.handler(new LoggingHandler(LogLevel.DEBUG)).childHandler(new ChannelInitializer<SocketChannel>() {

					@Override
					protected void initChannel(SocketChannel ch) throws Exception {
						ChannelPipeline p = ch.pipeline();
						p.addLast(new LengthFieldBasedFrameDecoder(1024, 0, 4, 0, 4));
						p.addLast(new KryoObjectDecoder());
						p.addLast(new IWCHandler(router));
					}

				}).bind(address, port).sync().channel();
	}

	public void stop() throws InterruptedException {
		channel.closeFuture().await();
	}

	public static void main(String[] args) throws Exception {
		new InternalTCPTransportServer(null, 12552).start();
	}
}
