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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.google.gson.Gson;
import com.srotya.linea.clustering.ClusterKeeper;
import com.srotya.linea.clustering.Columbus;
import com.srotya.linea.clustering.WorkerEntry;

/**
 * Zookeeper based {@link ClusterKeeper} implementation. This implementation
 * uses Apache Zookeeper to populate worker information across the swarm.<br>
 * <br>
 * Information is stored in a ZNode as JSON object (so it can be inspected
 * manually) and is polled by {@link Columbus} periodically updating last ping
 * times and worker availability.
 * 
 * @author ambud
 */
public class ZookeeperClusterKeeper implements ClusterKeeper, Watcher {

	private static final Logger logger = Logger.getLogger(ZookeeperClusterKeeper.class.getName());
	private static final String LINEA_ZK_ROOT = "linea.zk.root";
	private static final String DEFAULT_ZK_CONNECTION_STRNG = "localhost:2181";
	private static final String DEFAULT_ZK_ROOT = "/linea";
	private static final String LINEA_ZK_CONNECTION_STRING = "linea.zk.connectionString";
	private String zkRoot;
	private ZooKeeper zk;
	private boolean autoResetZk = false;
	private String zkConnectionString;

	@Override
	public void init(Map<String, String> conf) throws Exception {
		zkRoot = conf.getOrDefault(LINEA_ZK_ROOT, DEFAULT_ZK_ROOT);
		zkConnectionString = conf.getOrDefault(LINEA_ZK_CONNECTION_STRING, DEFAULT_ZK_CONNECTION_STRNG);
		try {
			zk = new ZooKeeper(zkConnectionString, 60000, this);
			logger.info("Connected to zookeeper using connection string:" + zkConnectionString);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		if (autoResetZk) {
			Stat exists = zk.exists(zkRoot, false);
			if (exists != null) {
				ZKUtil.deleteRecursive(zk, zkRoot);
			}
		}
		Stat exists = zk.exists(zkRoot, false);
		if (exists == null) {
			logger.info("Missing ZkRoot, creating it:" + zkRoot);
			zk.create(zkRoot, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
	}

	@Override
	public Map<Integer, WorkerEntry> pollWorkers() throws Exception {
		Map<Integer, WorkerEntry> entries = new HashMap<>();
		Gson gson = new Gson();
		List<String> children = zk.getChildren(zkRoot, true);
		for (String child : children) {
			child = zkRoot + "/" + child;
			Stat stat = zk.exists(child, true);
			byte[] data = zk.getData(child, true, stat);
			WorkerEntry value = gson.fromJson(new String(data), WorkerEntry.class);
			child = child.substring(child.lastIndexOf("/") + 1, child.length());
			entries.put(Integer.parseInt(child), value);
		}
		return entries;
	}

	@Override
	public void process(WatchedEvent event) {
	}

	@Override
	public int registerWorker(int selfWorkerId, WorkerEntry entry) throws Exception {
		Gson gson = new Gson();
		String id = zkRoot + "/" + selfWorkerId;
		Stat exists = zk.exists(id, this);
		if (exists == null) {
			if (selfWorkerId < 0) {
				id = zk.create(zkRoot + "/", gson.toJson(entry).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT_SEQUENTIAL);
				zk.delete(id, -1);
			} else {
				id = zk.create(zkRoot + "/" + selfWorkerId, gson.toJson(entry).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
			}
		}
		logger.fine("Created node:" + id + " to register this worker");
		id = id.substring(id.lastIndexOf("/") + 1, id.length());
		selfWorkerId = Integer.parseInt(id);
		logger.fine("Written worker entry for this worker:" + gson.toJson(entry) + "\t" + entry.getWorkerAddress());
		return selfWorkerId;
	}

}
