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
package com.srotya.linea.network.prototype;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.List;

import com.srotya.linea.Event;
import com.srotya.linea.network.KryoObjectDecoder;
import com.srotya.linea.network.Router;
import com.srotya.linea.utils.Constants;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * @author ambud
 */
public class InternalUDPTransportServer2 {

	// private static final Logger logger =
	// Logger.getLogger(InternalUDPTransportServer.class.getName());
	public static final boolean SSL = System.getProperty("ssl") != null;

	private DatagramSocket soc;
	private Router router;
	private int port;

	public InternalUDPTransportServer2(Router router, int port) {
		this.router = router;
		this.port = port;
	}

	public void start() throws Exception {
		soc = new DatagramSocket(port);
		byte[] buf = new byte[1500];
		while (true) {
			DatagramPacket p = new DatagramPacket(buf, buf.length);
			soc.receive(p);
			ByteBuf b = Unpooled.wrappedBuffer(buf);
			List<Event> events = KryoObjectDecoder.bytebufToEvents(b);
			for (Event event : events) {
				router.directLocalRouteEvent(event.getHeaders().get(Constants.FIELD_NEXT_BOLT).toString(),
						(Integer) event.getHeaders().get(Constants.FIELD_DESTINATION_TASK_ID), event);
			}
		}
	}

	public void stop() throws InterruptedException {
		soc.close();
	}

}
