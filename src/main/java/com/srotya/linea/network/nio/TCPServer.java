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
package com.srotya.linea.network.nio;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.esotericsoftware.kryo.io.Input;
import com.srotya.linea.Event;
import com.srotya.linea.network.KryoObjectDecoder;
import com.srotya.linea.network.Router;
import com.srotya.linea.utils.Constants;

/**
 * @author ambud
 */
public class TCPServer {

	private ServerSocket server;
	private ExecutorService es;
	private Router router;
	private int dataPort;
	private String bindAddress;
	
	public TCPServer(Router router, String bindAddress, int dataPort) {
		this.router = router;
		this.bindAddress = bindAddress;
		this.dataPort = dataPort;
	}
	
	public void start() throws Exception {
		es = Executors.newCachedThreadPool();
		server = new ServerSocket(dataPort, 10, InetAddress.getByName(bindAddress));
		
		while(true) {
			final Socket socket = server.accept();
			socket.setReceiveBufferSize(1048576);
			es.submit(()->{
				try {
					InputStream stream = new BufferedInputStream(socket.getInputStream(), 4096);
					Input input = new Input(stream);
					while(true) {
						Event event = KryoObjectDecoder.streamToEvent(input);
//						System.err.println("Recvd:"+event);
						router.directLocalRouteEvent(event.getHeaders().get(Constants.FIELD_NEXT_BOLT).toString(),
								(Integer) event.getHeaders().get(Constants.FIELD_DESTINATION_TASK_ID), event);
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});
		}
	}
	
	public void stop() throws IOException {
		server.close();
	}

}
