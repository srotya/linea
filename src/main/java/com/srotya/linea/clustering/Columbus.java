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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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
	private static final String DEFAULT_KEEPER_CLASS = "com.srotya.linea.clustering.columbus.ZookeeperClusterKeeper";//"com.srotya.linea.clustering.columbus.FaultTolerantClusterKeeper";// 
	private static final Logger logger = Logger.getLogger(Columbus.class.getName());
	private AtomicInteger workerCount = new AtomicInteger(0);
	private Map<Integer, WorkerEntry> workerMap;
	private InetAddress address;
	private int dataPort;
	private volatile int selfWorkerId;
	private ClusterKeeper keeper;
	private File idCacheFile;

	/**
	 * @param conf
	 * @throws IOException
	 */
	public Columbus(Map<String, String> conf) throws IOException {
		this.dataPort = Integer.parseInt(conf.getOrDefault(Topology.WORKER_DATA_PORT, Topology.DEFAULT_DATA_PORT));
		logger.info("Using worker data port:" + dataPort);
		this.address = InetAddress
				.getByName(conf.getOrDefault(Topology.WORKER_BIND_ADDRESS, Topology.DEFAULT_BIND_ADDRESS));// NetworkUtils.getIPv4Address(iface);
		this.workerMap = new ConcurrentHashMap<>();

		this.selfWorkerId = Integer.parseInt(conf.getOrDefault(Topology.WORKER_ID, "-1"));
		this.idCacheFile = new File("./target/.idCache");
		// check cache, uses the same logic as
		// https://issues.apache.org/jira/browse/KAFKA-1070
		if (selfWorkerId < 0) {
			if (idCacheFile.exists()) {
				String workerId = new String(Files.readAllBytes(idCacheFile.toPath()), Charset.forName("utf-8"));
				if (workerId.length() > 0) {
					selfWorkerId = Integer.parseInt(workerId);
				}
			}
		}

		String keeperClass = conf.getOrDefault(KEEPER_CLASS_FQCN, DEFAULT_KEEPER_CLASS);
		try {
			keeper = (ClusterKeeper) Class.forName(keeperClass).newInstance();
			keeper.init(conf, address);
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			logger.log(Level.SEVERE, "Invalid Keeper class", e);
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Failed to initialize Keeper", e);
		}
	}

	/**
	 * Add a known peer to the worker map and increment worker count by 1.
	 * 
	 * @param workerId
	 * @param peer
	 * @param discoveryPort
	 * @param dataPort
	 */
	public void addKnownPeer(int workerId, InetAddress peer, int discoveryPort, int dataPort) {
		WorkerEntry entry = new WorkerEntry(peer, discoveryPort, dataPort);
		workerMap.put(workerId, entry);
		workerCount.incrementAndGet();
	}

	/**
	 * Check if worker id already exists and update the last discovered time
	 * else create an entry in the worker map.
	 * 
	 * @param workerId
	 * @param entry
	 */
	public void addKnownPeer(int workerId, WorkerEntry entry) {
		if (workerMap.containsKey(workerId)) {
			logger.fine("Updating worker entry for worker id:" + workerId + "\t" + entry.getWorkerAddress());
			workerMap.get(workerId).setLastContactTimestamp(System.currentTimeMillis());
		} else {
			workerMap.put(workerId, entry);
			workerCount.incrementAndGet();
			logger.fine("Added worker entry for worker id:" + workerId + "\t" + entry.getWorkerAddress());
		}
	}

	@Override
	public void run() {
		// monitor
		while (true) {
			try {
				WorkerEntry thisWorker = workerMap.get(selfWorkerId);
				if (thisWorker == null) {
					thisWorker = new WorkerEntry(address, dataPort, System.currentTimeMillis());
				} else {
					thisWorker.setLastContactTimestamp(System.currentTimeMillis());
				}
				selfWorkerId = keeper.registerWorker(selfWorkerId, thisWorker);
				// update id cache file
				Files.write(idCacheFile.toPath(), String.valueOf(selfWorkerId).getBytes(), StandardOpenOption.WRITE,
						StandardOpenOption.CREATE);
				addKnownPeer(selfWorkerId, thisWorker);
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Exception registering worker", e);
			}
			try {
				// poll workers and
				Map<Integer, WorkerEntry> entries = keeper.pollWorkers();
				if (entries != null) {
					for (Entry<Integer, WorkerEntry> entry : entries.entrySet()) {
						logger.fine("Discovered worker:" + entry.getValue() + " with id:" + entry.getKey());
						addKnownPeer(entry.getKey(), entry.getValue());
					}
				}
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Exception polling worker information", e);
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				break;
			}
		}
	}

	/**
	 * @return id for this worker
	 */
	public Integer getSelfWorkerId() {
		return selfWorkerId;
	}

	/**
	 * @return worker map
	 */
	public Map<Integer, WorkerEntry> getWorkerMap() {
		return workerMap;
	}

	/**
	 * @return worker count
	 */
	public int getWorkerCount() {
		return workerCount.get();
	}

}