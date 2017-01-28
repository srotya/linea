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
package com.srotya.linea.clustering.columbus;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import com.srotya.linea.clustering.ClusterKeeper;
import com.srotya.linea.clustering.WorkerEntry;

/**
 * Simple {@link ClusterKeeper} for testing
 * 
 * @author ambud
 */
public class SingleNodeClusterKeeper implements ClusterKeeper {

	private WorkerEntry entry;

	@Override
	public void init(Map<String, String> conf, InetAddress address) throws Exception {
	}

	@Override
	public int registerWorker(int selfWorkerId, WorkerEntry entry) throws Exception {
		this.entry = entry;
		return 0;
	}

	@Override
	public Map<Integer, WorkerEntry> pollWorkers() throws Exception {
		Map<Integer, WorkerEntry> map = new HashMap<>();
		map.put(0, entry);
		return map;
	}

}
