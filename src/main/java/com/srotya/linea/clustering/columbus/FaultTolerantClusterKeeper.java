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

import java.util.Map;
import java.util.logging.Logger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.google.gson.Gson;
import com.srotya.linea.clustering.ClusterKeeper;
import com.srotya.linea.clustering.Columbus;
import com.srotya.linea.clustering.WorkerEntry;

/**
 * @author ambud
 */
public class FaultTolerantClusterKeeper implements ClusterKeeper, Watcher {

	private static final Logger logger = Logger.getLogger(FaultTolerantClusterKeeper.class.getName());
	private static final String WORKERS = "/workers";
	private String basePath;
	private Columbus columbus;
	private ZooKeeper zk;

	@Override
	public void init(Map<String, String> conf, Columbus columbus) throws Exception {
		this.columbus = columbus;
		zk = new ZooKeeper(conf.getOrDefault("zk.conn.str", "localhost:2181"), 60000, this);
		logger.info("Connected to zookeeper");
		basePath = conf.getOrDefault("zk.base.path", "/linea");
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

	}

	@Override
	public int updateWorkerEntry(WorkerEntry entry) throws Exception {
		Gson gson = new Gson();
		String json = gson.toJson(entry);
		String workerZnode = basePath + WORKERS + "/" + entry.getWorkerAddress().getHostAddress() + ":"
				+ entry.getDataPort();
		Stat ls = zk.exists(workerZnode, this);
		if (ls == null) {
			zk.create(workerZnode, json.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		} else {
			ls = zk.setData(workerZnode, json.getBytes(), -1);
		}
		return 0;
	}

	@Override
	public synchronized void process(WatchedEvent event) {
		if (!event.getPath().contains(basePath + WORKERS)) {
			// ignore event
			System.out.println("Ignoring event for base path:" + event.getPath());
			return;
		}
		if (event.getType() == EventType.NodeCreated) {
			try {
				Gson gson = new Gson();
				Stat stat = zk.exists(event.getPath(), false);
				if (stat != null) {
					byte[] data = zk.getData(event.getPath(), false, stat);
					WorkerEntry entry = gson.fromJson(new String(data), WorkerEntry.class);
					columbus.addWorker(entry);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else if (event.getType() == EventType.NodeDeleted) {
			System.out.println("Worker died:" + event.getPath());
		}
	}

	@Override
	public void notifyWorkerFailure(WorkerEntry entry) throws Exception {
		String zkRoot = basePath + WORKERS;
		System.out.println("Worker failure:" + entry);
		zk.delete(zkRoot + "/" + entry.getWorkerAddress().getHostAddress() + ":" + entry.getDataPort(), -1);
	}

}
