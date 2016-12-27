package com.srotya.linea.network;

import com.srotya.linea.Event;
import com.srotya.linea.utils.Constants;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * IWC or Inter-Worker Communication Handler is the last Handler in the
 * Netty Pipeline for receiving {@link Event}s from other workers.
 * 
 * @author ambud
 */
public class IWCHandler extends ChannelInboundHandlerAdapter {

	private Router router;

	public IWCHandler(Router router) {
		this.router = router;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) {
		Event event = (Event) msg;
//		System.err.println("Receive:"+event);
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