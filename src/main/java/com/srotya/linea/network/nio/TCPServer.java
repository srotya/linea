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
package com.srotya.linea.network.nio;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.srotya.linea.Tuple;
import com.srotya.linea.network.KryoObjectDecoder;
import com.srotya.linea.network.Router;

/**
 * Inter Worker Communication (IWC) server.
 * 
 * @author ambud
 */
public class TCPServer<E extends Tuple> {

	private ServerSocket server;
	private ExecutorService es;
	private Router<E> router;
	private int dataPort;
	private String bindAddress;
	private Class<E> classOf;
	public static final ThreadLocal<Kryo> kryoThreadLocal = new ThreadLocal<Kryo>() {
		@Override
		protected Kryo initialValue() {
			Kryo kryo = new Kryo();
			return kryo;
		}
	};

	public TCPServer(Class<E> classOf, Router<E> router, String bindAddress, int dataPort) {
		this.classOf = classOf;
		this.router = router;
		this.bindAddress = bindAddress;
		this.dataPort = dataPort;
	}

	public void start() throws Exception {
		es = Executors.newCachedThreadPool();
		server = new ServerSocket(dataPort, 10, InetAddress.getByName(bindAddress));

		while (true) {
			final Socket socket = server.accept();
			socket.setReceiveBufferSize(1048576);
			es.submit(new Thread() {
				public void run() {
					try {
						InputStream stream = new BufferedInputStream(socket.getInputStream(), 4096);
						Input input = new Input(stream);
						while (true) {
							E event = KryoObjectDecoder.streamToEvent(classOf, input);
							router.directLocalRouteEvent(event.getNextBoltId(), event.getDestinationTaskId(), event);
						}
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			});
		}
	}

	public void stop() throws IOException {
		server.close();
	}

}
