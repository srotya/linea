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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.srotya.linea.clustering.ClusterKeeper;
import com.srotya.linea.clustering.WorkerEntry;

/**
 * @author ambud
 */
public class FaultTolerantClusterKeeper implements ClusterKeeper, Watcher, LeaderSelectorListener {

	private static final String LE_PATH = "/le";
	private static final Logger logger = Logger.getLogger(FaultTolerantClusterKeeper.class.getName());
	private static final String WORKERS = "/workers";
	private static final String LEADER_PATH = "/leader";
	private CuratorFramework curatorClient;
	private LeaderSelector leaderSelector;
	private String basePath;
	private InetAddress selfAddress;
	private AtomicInteger assignedWorkerId;
	private AtomicBoolean isLeader;
	private Map<String, Integer> leaderAddressMap;

	@Override
	public void init(Map<String, String> conf, InetAddress selfAddress) throws Exception {
		this.selfAddress = selfAddress;
		curatorClient = CuratorFrameworkFactory.newClient(conf.getOrDefault("zk.conn.str", "localhost:2181"), 60000,
				5000, new ExponentialBackoffRetry(3000, 3));
		curatorClient.start();
		logger.info("Connected to zookeeper");
		basePath = conf.getOrDefault("zk.base.path", "/linea");
		ZooKeeper zk = curatorClient.getZookeeperClient().getZooKeeper();
		Stat exists = zk.exists(basePath, false);
		if (exists == null) {
			zk.create(basePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			logger.info("Missing base path:" + basePath + " created");
		}
		exists = zk.exists(basePath + WORKERS, false);
		if (exists == null) {
			zk.create(basePath + WORKERS, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			logger.info("Missing worker path:" + basePath + WORKERS + " created");
		}
		exists = zk.exists(basePath + LE_PATH, false);
		if (exists == null) {
			zk.create(basePath + LE_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			logger.info("Missing leader election path:" + basePath + LE_PATH + " created");
		}

		leaderSelector = new LeaderSelector(curatorClient, basePath + LE_PATH, this);
		leaderSelector.autoRequeue();
		leaderSelector.start();
		logger.info("Started leader election process");

		AtomicBoolean bool = new AtomicBoolean(false);
		while (!bool.get()) {
			try {
				Stat leader = zk.exists(basePath + LEADER_PATH, false);
				if (leader == null) {
					logger.info("Waiting for leader election");
				} else {
					bool.set(true);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			Thread.sleep(1000);
		}
		assignedWorkerId = new AtomicInteger(-1);
		isLeader = new AtomicBoolean(false);
		leaderAddressMap = new ConcurrentHashMap<>();
	}

	@Override
	public int registerWorker(int selfWorkerId, WorkerEntry entry) throws Exception {
		assignedWorkerId.set(-1);
		entry.setWorkerId(-1);
		ZooKeeper zk = curatorClient.getZookeeperClient().getZooKeeper();
		Gson gson = new Gson();
		String json = gson.toJson(entry);
		String workerZnode = basePath + WORKERS + "/" + entry.getWorkerAddress().getHostAddress() + ":"
				+ entry.getDataPort();
		Stat ls = zk.exists(workerZnode, false);
		if (ls == null) {
			zk.create(workerZnode, json.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} else {
			ls = zk.setData(workerZnode, json.getBytes(), -1);
		}
		zk.exists(workerZnode, this);
		while (assignedWorkerId.get() == -1) {
			// poll for worker id assignment
			logger.info("Waiting for leader to assign an id to worker:" + entry.getWorkerAddress());
			Thread.sleep(1000);
		}
		entry.setWorkerId(assignedWorkerId.get());
		logger.info("Leader assigned the id:" + assignedWorkerId.get() + " to worker:" + entry.getWorkerAddress());
		return assignedWorkerId.get();
	}

	@Override
	public Map<Integer, WorkerEntry> pollWorkers() throws Exception {
		ZooKeeper zk = curatorClient.getZookeeperClient().getZooKeeper();
		String zkRoot = basePath + LEADER_PATH;
		Gson gson = new Gson();
		Map<Integer, WorkerEntry> entries = new HashMap<>();
		List<String> children = zk.getChildren(zkRoot, false);
		for (String child : children) {
			child = zkRoot + "/" + child;
			Stat stat = zk.exists(child, true);
			byte[] data = zk.getData(child, true, stat);
			WorkerEntry value = gson.fromJson(new String(data), WorkerEntry.class);
			child = child.substring(child.lastIndexOf("/") + 1, child.length());
			entries.put(value.getWorkerId(), value);
		}
		return entries;
	}

	@Override
	public void stateChanged(CuratorFramework arg0, ConnectionState state) {
		logger.fine("Connection state changed:" + state);
	}

	@Override
	public void takeLeadership(CuratorFramework leader) throws Exception {
		logger.info("Leader elected!" + selfAddress);
		isLeader.set(true);
		ZooKeeper zk = leader.getZookeeperClient().getZooKeeper();
		Gson gson = new Gson();
		JsonObject object = new JsonObject();
		object.addProperty("address", selfAddress.getHostAddress());
		String leaderData = gson.toJson(object);
		Stat ls = zk.exists(basePath + LEADER_PATH, false);
		if (ls == null) {
			zk.create(basePath + LEADER_PATH, leaderData.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		} else {
			// update znode with this machines's leader info
			zk.setData(basePath + LEADER_PATH, leaderData.getBytes(), -1);
		}
		String zkRoot = basePath + WORKERS;
		while (isLeader.get()) {
			List<String> children = zk.getChildren(zkRoot, false);
			Collections.sort(children);
			int workerId = 0;
			for (String child : children) {
				String ip = child;
				child = zkRoot + "/" + child;
				Stat stat = zk.exists(child, false);
				byte[] data = zk.getData(child, false, stat);
				WorkerEntry value = gson.fromJson(new String(data), WorkerEntry.class);
				if (System.currentTimeMillis() - value.getLastContactTimestamp() >= 5000) {
					leaderAddressMap.remove(ip);
					continue;
				}
				leaderAddressMap.put(ip, workerId);
				value.setWorkerId(workerId);
				zk.setData(basePath + WORKERS + "/" + value.getWorkerAddress().getHostAddress(),
						gson.toJson(value).getBytes(), ls.getVersion() + 1);
				workerId++;
			}
			Thread.sleep(1000);
		}
		isLeader.set(false);
	}

	@Override
	public synchronized void process(WatchedEvent event) {
		if (!event.getPath().contains(basePath + WORKERS)) {
			// ignore event
			System.out.println("Ignoring event for base path:" + event.getPath());
			return;
		}
		System.out.println("Zk path:" + event.getPath());
		if (event.getType() == EventType.NodeDataChanged) {
			if (!event.getPath().contains(selfAddress.getHostAddress())) {
				// ignore event
				return;
			}
			try {
				Gson gson = new Gson();
				ZooKeeper zk = curatorClient.getZookeeperClient().getZooKeeper();
				Stat stat = zk.exists(event.getPath(), false);
				if (stat != null) {
					byte[] data = zk.getData(event.getPath(), false, stat);
					WorkerEntry entry = gson.fromJson(new String(data), WorkerEntry.class);
					assignedWorkerId.set(entry.getWorkerId());
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
