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
package com.srotya.linea.clustering;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.srotya.linea.Topology;

/**
 * The worker discovery service of Linea. Columbus runs as a daemon and
 * periodically polls the {@link ClusterKeeper} to find information about
 * discovered workers.
 * 
 * @author ambud
 */
public class Columbus implements Runnable {

	private static final String KEEPER_CLASS_FQCN = "linea.keeper.class";
	private static final String DEFAULT_KEEPER_CLASS = "com.srotya.linea.clustering.columbus.FaultTolerantClusterKeeper";
	private static final Logger logger = Logger.getLogger(Columbus.class.getName());
	private AtomicReference<List<WorkerEntry>> workerList = new AtomicReference<List<WorkerEntry>>(new ArrayList<>());
	private InetAddress address;
	private int dataPort;
	private ClusterKeeper keeper;
	private WorkerEntry selfWorkerEntry;
	private int selfWorkerId;

	/**
	 * @param conf
	 * @throws IOException
	 */
	public Columbus(Map<String, String> conf) throws IOException {
		this.dataPort = Integer.parseInt(conf.getOrDefault(Topology.WORKER_DATA_PORT, Topology.DEFAULT_DATA_PORT));
		logger.info("Using worker data port:" + dataPort);
		this.address = InetAddress
				.getByName(conf.getOrDefault(Topology.WORKER_BIND_ADDRESS, Topology.DEFAULT_BIND_ADDRESS));// NetworkUtils.getIPv4Address(iface);

		selfWorkerEntry = new WorkerEntry(address, dataPort, System.currentTimeMillis());

		String keeperClass = conf.getOrDefault(KEEPER_CLASS_FQCN, DEFAULT_KEEPER_CLASS);
		try {
			keeper = (ClusterKeeper) Class.forName(keeperClass).newInstance();
			keeper.init(conf, this);
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			logger.log(Level.SEVERE, "Invalid Keeper class", e);
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Failed to initialize Keeper", e);
		}
	}

	@Override
	public void run() {
		while (true) {
			// Push update status every 10 seconds
			try {
				selfWorkerEntry.setLastContactTimestamp(System.currentTimeMillis());
				keeper.updateWorkerEntry(selfWorkerEntry);
				Thread.sleep(1000);
			} catch (Exception e) {
				break;
			}
		}
	}

	public List<WorkerEntry> getWorkerList() {
		return workerList.get();
	}

	public void addWorker(WorkerEntry entry) {
		List<WorkerEntry> list = new ArrayList<>(workerList.get());
		list.add(entry);
		Collections.sort(list);
		selfWorkerId = Collections.binarySearch(list, selfWorkerEntry);
		workerList.set(list);
	}

	public void removeWorker(WorkerEntry entry) {
		List<WorkerEntry> list = new ArrayList<>(workerList.get());
		list.remove(entry);
		Collections.sort(list);
		selfWorkerId = Collections.binarySearch(list, selfWorkerEntry);
		workerList.set(list);
	}

	public WorkerEntry getWorkerForHashValue(int hashValue) {
		List<WorkerEntry> list = workerList.get();
		return list.get(hashValue % list.size());
	}

	public int size() {
		return workerList.get().size();
	}

	public WorkerEntry getSelfWorker() {
		return selfWorkerEntry;
	}

	public WorkerEntry getByWorkerByIndex(int destinationWorker) {
		return workerList.get().get(destinationWorker);
	}

	public int getSelfWorkerId() {
		return selfWorkerId;
	}

}