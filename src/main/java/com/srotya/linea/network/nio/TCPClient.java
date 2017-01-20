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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import com.lmax.disruptor.EventHandler;
import com.srotya.linea.Tuple;
import com.srotya.linea.clustering.Columbus;
import com.srotya.linea.clustering.WorkerEntry;
import com.srotya.linea.network.KryoObjectEncoder;

/**
 * @author ambud
 */
public class TCPClient<E extends Tuple> implements EventHandler<E> {

	private static final Logger logger = Logger.getLogger(TCPClient.class.getName());
	private Map<Integer, OutputStream> socketMap;
	private Columbus columbus;
	private int clientThreads;
	private int clientThreadId;

	public TCPClient(Columbus columbus, int clientThreadId, int clientThreads) {
		this.columbus = columbus;
		this.clientThreadId = clientThreadId;
		this.clientThreads = clientThreads;
		socketMap = new HashMap<>();
	}

	public void start() throws Exception {
		for (Entry<Integer, WorkerEntry> entry : columbus.getWorkerMap().entrySet()) {
			if (entry.getKey() != columbus.getSelfWorkerId()) {
				Integer key = entry.getKey();
				WorkerEntry value = entry.getValue();
				retryConnectLoop(key, value);
			}
		}
	}

	private void retryConnectLoop(Integer key, WorkerEntry value) throws InterruptedException, IOException {
		boolean connected = false;
		int retryCount = 1;
		while (!connected && retryCount < 100) {
			Socket socket = tryConnect(value, retryCount);
			if (socket != null) {
				connected = true;
				socketMap.put(key, new BufferedOutputStream(socket.getOutputStream(), 8192 * 4));
			}
		}
	}

	private Socket tryConnect(WorkerEntry value, int retryCount) throws InterruptedException {
		try {
			Socket socket = new Socket(value.getWorkerAddress(), value.getDataPort());
			socket.setSendBufferSize(1048576);
			socket.setKeepAlive(true);
			socket.setPerformancePreferences(0, 1, 2);
			return socket;
		} catch (Exception e) {
			logger.warning("Worker connection refused:" + value.getWorkerAddress() + ". Retrying in " + retryCount
					+ " seconds.....");
			retryCount++;
			Thread.sleep(1000 * retryCount);
		}
		return null;
	}

	@Override
	public void onEvent(E event, long sequence, boolean endOfBatch) throws Exception {
		int workerId = event.getDestinationWorkerId();
		try {
			if (workerId % clientThreads == clientThreadId) {
				OutputStream stream = socketMap.get(workerId);
				byte[] bytes = KryoObjectEncoder.eventToByteArray(event);
				stream.write(bytes);
				if (endOfBatch) {
					stream.flush();
				}
			}
		} catch (IOException e) {
			WorkerEntry entry = columbus.getWorkerMap().get(workerId);
			logger.severe("Lost worker connection to WorkerId:"+workerId+"\tAddress:"+entry.getWorkerAddress());
			retryConnectLoop(workerId, entry);
			if (workerId % clientThreads == clientThreadId) {
				OutputStream stream = socketMap.get(workerId);
				byte[] bytes = KryoObjectEncoder.eventToByteArray(event);
				stream.write(bytes);
				if (endOfBatch) {
					stream.flush();
				}
			}
		}
	}

}
