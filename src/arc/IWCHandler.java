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
package com.srotya.linea.network;

import com.srotya.linea.Event;

/**
 * IWC or Inter-Worker Communication Handler is the last Handler in the
 * Netty Pipeline for receiving {@link Event}s from other workers.
 * 
 * @author ambud
 */
public class IWCHandler {


	public IWCHandler(Router router) {
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) {
		Event event = (Event) msg;
		router.directLocalRouteEvent(event.getHeaders().get(Constants.FIELD_NEXT_BOLT).toString(),
				(Integer) event.getHeaders().get(Constants.FIELD_DESTINATION_TASK_ID), event);
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