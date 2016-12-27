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
package com.srotya.linea.clustering;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.google.gson.Gson;
import com.srotya.linea.topology.TopologyBuilder;
import com.srotya.linea.utils.NetworkUtils;

/**
 * Worker discovery service of Linea.
 * 
 * @author ambud
 */
public class Columbus implements Runnable, Watcher {

	private static final Logger logger = Logger.getLogger(Columbus.class.getName());
	private AtomicInteger workerCount = new AtomicInteger(0);
	private Map<Integer, WorkerEntry> workerMap;
	private InetAddress address;
	private int dataPort;
	private int selfWorkerId;
	// private Map<String, String> conf;
	private ZooKeeper zk;
	private boolean autoResetZk = false;

	public Columbus(Map<String, String> conf) throws IOException {
		// this.conf = conf;
		NetworkInterface iface = NetworkUtils.selectDefaultIPAddress(true);
		logger.info("Auto-selected network interface:" + iface);
		this.address = InetAddress.getByName(conf.getOrDefault("bind.address", "localhost"));//NetworkUtils.getIPv4Address(iface);
		this.workerMap = new ConcurrentHashMap<>();
		this.dataPort = Integer
				.parseInt(conf.getOrDefault(TopologyBuilder.WORKER_DATA_PORT, TopologyBuilder.DEFAULT_DATA_PORT));
		System.err.println("Worker data port:" + dataPort);
	}

	public void addKnownPeer(int workerId, InetAddress peer, int discoveryPort, int dataPort) {
		WorkerEntry entry = new WorkerEntry(peer, discoveryPort, dataPort);
		workerMap.put(workerId, entry);
		workerCount.incrementAndGet();
	}

	public void addKnownPeer(int workerId, WorkerEntry entry) {
		if (workerMap.containsKey(workerId)) {
			workerMap.get(workerId).setLastContactTimestamp(System.currentTimeMillis());
		} else {
			workerMap.put(workerId, entry);
			workerCount.incrementAndGet();
		}
	}

	@Override
	public void run() {
		try {
			zk = new ZooKeeper("localhost:2181", 60000, this);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		Gson gson = new Gson();

		// int ipAddress = NetUtils.stringIPtoInt(address.getHostAddress());
		// register this worker
		try {
			if (autoResetZk) {
				Stat exists = zk.exists("/linea", false);
				if (exists != null) {
					ZKUtil.deleteRecursive(zk, "/linea");
				}
			}

			Stat exists = zk.exists("/linea", false);
			if (exists == null) {
				zk.create("/linea", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}

			WorkerEntry thisWorker = new WorkerEntry(address, dataPort, System.currentTimeMillis());
			System.out.println("Written worker entry for this worker:"+gson.toJson(thisWorker)+"\t"+address);
			String id = zk.create("/linea/", gson.toJson(thisWorker).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT_SEQUENTIAL);
			System.out.println("Created node:" + id + " to register this worker");
			id = id.substring(id.lastIndexOf("/") + 1, id.length());
			selfWorkerId = Integer.parseInt(id);
			addKnownPeer(selfWorkerId, thisWorker);
		} catch (Exception e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		// monitor
		while (true) {
			try {
				List<String> children = zk.getChildren("/linea", true);
				for (String child : children) {
					child = "/linea/" + child;
					// System.out.println("Paths:" + child);
					Stat stat = zk.exists(child, true);
					byte[] data = zk.getData(child, true, stat);
					WorkerEntry value = gson.fromJson(new String(data), WorkerEntry.class);
					child = child.substring(child.lastIndexOf("/") + 1, child.length());
					addKnownPeer(Integer.parseInt(child), value);
					// System.err.println("Found entry:" + child);
				}
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
		}
	}

	public Integer getSelfWorkerId() {
		return selfWorkerId;
	}

	public Map<Integer, WorkerEntry> getWorkerMap() {
		return workerMap;
	}

	public int getWorkerCount() {
		return workerCount.get();
	}

	@Override
	public void process(WatchedEvent event) {
	}

}