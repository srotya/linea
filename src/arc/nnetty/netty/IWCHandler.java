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

import com.srotya.linea.Tuple;
import com.srotya.linea.network.Router;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * IWC or Inter-Worker Communication Handler is the last Handler in the Netty
 * Pipeline for receiving {@link Event}s from other workers.
 * 
 * @author ambud
 */
public class IWCHandler<E extends Tuple> extends SimpleChannelInboundHandler<E> {

	private Router<E> router;

	public IWCHandler(Router<E> router) {
		this.router = router;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, E msg) throws Exception {
		E tuple = (E) msg;
		router.directLocalRouteEvent(tuple.getNextBoltId(), tuple.getDestinationTaskId(), tuple);
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) {
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		cause.printStackTrace();
		ctx.close();
	}

}