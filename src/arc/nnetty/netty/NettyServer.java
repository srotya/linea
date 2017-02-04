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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.srotya.linea.Tuple;
import com.srotya.linea.network.NetworkServer;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.EventExecutor;

/**
 * @author ambud
 */
public class NettyServer<E extends Tuple> extends NetworkServer<E> {

	private ExecutorService es;

	private ChannelFuture channelFuture;

	public NettyServer() {
	}

	public void start() throws InterruptedException {
		EventLoopGroup bossGroup = new NioEventLoopGroup(1); // (1)
		EventLoopGroup workerGroup = new NioEventLoopGroup(1);
		es = Executors.newCachedThreadPool();
		final EventExecutor e2 = new DefaultEventExecutor(es);
		try {
			ServerBootstrap bootstrap = new ServerBootstrap(); // (2)
			bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class) // (3)
					.childHandler(new ChannelInitializer<SocketChannel>() { // (4)
						@Override
						public void initChannel(SocketChannel ch) throws Exception {
							ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(4096, 0, 2, 0, 2))
									.addLast(new NettyKryoObjectDecoder<E>(getClassOf()))
									.addLast(e2, new IWCHandler<>(getRouter()));
						}
					}).option(ChannelOption.SO_BACKLOG, 128) // (5)
					.option(ChannelOption.SO_RCVBUF, 1024 * 1024 * 4).childOption(ChannelOption.SO_KEEPALIVE, true); // (6)

			bootstrap.option(ChannelOption.TCP_NODELAY, false);
			bootstrap.option(ChannelOption.SO_REUSEADDR, true);
			bootstrap.option(ChannelOption.SO_LINGER, 0);
			bootstrap.childOption(ChannelOption.TCP_NODELAY, false);
			bootstrap.childOption(ChannelOption.SO_REUSEADDR, true);
			bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
			bootstrap.childOption(ChannelOption.SO_LINGER, 0);
			bootstrap.childOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000);
			bootstrap.childOption(ChannelOption.SO_RCVBUF, 1048576);
			bootstrap.childOption(ChannelOption.SO_SNDBUF, 1048576);
			bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
			channelFuture = bootstrap.bind(getBindAddress(), getDataPort()).sync();

			// Wait until the server socket is closed.
			// In this example, this does not happen, but you can do that to
			// gracefully
			// shut down your server.
			channelFuture.channel().closeFuture().sync();
		} finally {
			workerGroup.shutdownGracefully();
			bossGroup.shutdownGracefully();
		}
	}

	public void stop() throws IOException {
	}

}
