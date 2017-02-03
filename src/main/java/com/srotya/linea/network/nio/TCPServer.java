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

import com.esotericsoftware.kryo.io.Input;
import com.srotya.linea.Tuple;
import com.srotya.linea.network.KryoObjectDecoder;
import com.srotya.linea.network.NetworkServer;

/**
 * Inter Worker Communication (IWC) server.
 * 
 * @author ambud
 */
public class TCPServer<E extends Tuple> extends NetworkServer<E> {

	private ServerSocket server;
	private ExecutorService es;

	public TCPServer() {
	}

	public void start() throws Exception {
		es = Executors.newFixedThreadPool(getColumbus().getWorkerCount());
		server = new ServerSocket(getDataPort(), 100, InetAddress.getByName(getBindAddress()));
		System.err.println("TCP Server started");
		while (true) {
			if (Thread.currentThread().isInterrupted()) {
				break;
			}
			final Socket socket = server.accept();
			System.err.println("Connected to client:" + socket.getInetAddress());
			socket.setReceiveBufferSize(8192 * 4);
			es.submit(new Thread() {
				public void run() {
					try {
						InputStream stream = new BufferedInputStream(socket.getInputStream(), 4096);
						Input input = new Input(stream);
						while (true) {
							E event = KryoObjectDecoder.streamToEvent(getClassOf(), input);
							getRouter().directLocalRouteEvent(event.getNextBoltId(), event.getDestinationTaskId(),
									event);
						}
					} catch (IOException e) {
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
