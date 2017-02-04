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

import com.srotya.linea.Tuple;
import com.srotya.linea.clustering.WorkerEntry;
import com.srotya.linea.network.KryoCodec;
import com.srotya.linea.network.NetworkClient;

/**
 * Inter Worker Communication (IWC) client, implemented as a Disruptor handler.
 * 
 * @author ambud
 */
public class TCPClient<E extends Tuple> extends NetworkClient<E> {

	private Logger logger;
	private Map<Integer, OutputStream> socketMap;

	public TCPClient() {
		this.logger = Logger.getLogger(TCPClient.class.getName());
		socketMap = new HashMap<>();
	}

	public void start() throws Exception {
		for (Entry<Integer, WorkerEntry> entry : getColumbus().getWorkerMap().entrySet()) {
			if (entry.getKey() != getColumbus().getSelfWorkerId()) {
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
			retryCount++;
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
			Thread.sleep(1000 * retryCount);
		}
		return null;
	}

	@Override
	public void onEvent(E event, long sequence, boolean endOfBatch) throws Exception {
		int workerId = event.getDestinationWorkerId();
		try {
			if (workerId % getClientThreads() == getClientThreadId()) {
				OutputStream stream = socketMap.get(workerId);
				byte[] bytes = KryoCodec.eventToByteArray(event);
				stream.write(bytes);
				stream.flush();
			}
		} catch (Exception e) {
			WorkerEntry entry = getColumbus().getWorkerMap().get(workerId);
			logger.severe("Lost worker connection to WorkerId:" + workerId + "\tAddress:" + entry + "\treason:"
					+ e.getMessage());
			retryConnectLoop(workerId, entry);
			if (workerId % getClientThreads() == getClientThreadId()) {
				OutputStream stream = socketMap.get(workerId);
				byte[] bytes = KryoCodec.eventToByteArray(event);
				stream.write(bytes);
				if (endOfBatch) {
					stream.flush();
				}
			}
		}
	}

}
