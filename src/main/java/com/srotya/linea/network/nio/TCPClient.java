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

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.lmax.disruptor.EventHandler;
import com.srotya.linea.Event;
import com.srotya.linea.clustering.Columbus;
import com.srotya.linea.clustering.WorkerEntry;
import com.srotya.linea.network.KryoObjectEncoder;
import com.srotya.linea.utils.Constants;

/**
 * @author ambud
 */
public class TCPClient implements EventHandler<Event> {

	private Map<Integer, OutputStream> socketMap;
	private Columbus columbus;

	public TCPClient(Columbus columbus) {
		this.columbus = columbus;
		socketMap = new HashMap<>();
	}

	public void start() throws Exception {
		for (Entry<Integer, WorkerEntry> entry : columbus.getWorkerMap().entrySet()) {
			if (entry.getKey() != columbus.getSelfWorkerId()) {
				@SuppressWarnings("resource")
				Socket socket = new Socket(entry.getValue().getWorkerAddress(), entry.getValue().getDataPort());
				socketMap.put(entry.getKey(),
						new BufferedOutputStream(socket.getOutputStream()));
			}
		}
	}

	@Override
	public void onEvent(Event event, long sequence, boolean endOfBatch) throws Exception {
		Integer workerId = (Integer) event.getHeaders().get(Constants.FIELD_DESTINATION_WORKER_ID);
		OutputStream stream = socketMap.get(workerId);
		KryoObjectEncoder.writeEventToStream(event, stream);
		stream.flush();
	}

}
