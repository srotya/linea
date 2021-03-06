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
package com.srotya.linea.network;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.srotya.linea.MurmurHash;
import com.srotya.linea.Topology;
import com.srotya.linea.Tuple;
import com.srotya.linea.clustering.Columbus;
import com.srotya.linea.disruptor.CopyTranslator;
import com.srotya.linea.disruptor.ROUTING_TYPE;
import com.srotya.linea.network.netty.NettyClient;
import com.srotya.linea.network.netty.NettyServer;
import com.srotya.linea.network.nio.TCPClient;
import com.srotya.linea.network.nio.TCPServer;
import com.srotya.linea.processors.BoltExecutor;

/**
 * {@link Tuple} router that is responsible for sending messages across
 * instances and workers in a topology.
 * 
 * @author ambud
 */
public class Router<E extends Tuple> {

	private static final Logger logger = Logger.getLogger(Router.class.getName());
	private Disruptor<E> networkTranmissionDisruptor;
	private EventFactory<E> factory;
	private Map<String, BoltExecutor<E>> executorMap;
	private CopyTranslator<E> translator;
	private Columbus columbus;
	private NetworkServer<E> server;
	private int workerCount;
	private ExecutorService pool;
	private int dataPort;
	private List<NetworkClient<E>> clients;
	private String bindAddress;
	private int clientThreadCount;
	private Class<E> classOf;
	private String networkServerClass;
	private String networkClientClass;

	/**
	 * @param classOf
	 * @param factory
	 * @param columbus
	 * @param workerCount
	 * @param executorMap
	 * @param conf
	 * @param translator
	 */
	public Router(Class<E> classOf, EventFactory<E> factory, Columbus columbus, int workerCount,
			Map<String, BoltExecutor<E>> executorMap, Map<String, String> conf, CopyTranslator<E> translator) {
		this.classOf = classOf;
		this.factory = factory;
		this.columbus = columbus;
		this.workerCount = workerCount;
		this.executorMap = executorMap;
		this.translator = translator;

		this.networkServerClass = conf.getOrDefault("network.server.class", TCPServer.class.getName());
		this.networkClientClass = conf.getOrDefault("network.client.class", TCPClient.class.getName());

//		this.networkServerClass = conf.getOrDefault("network.server.class", NettyServer.class.getName());
//		this.networkClientClass = conf.getOrDefault("network.client.class", NettyClient.class.getName());

		this.bindAddress = conf.getOrDefault(Topology.WORKER_BIND_ADDRESS, Topology.DEFAULT_BIND_ADDRESS);
		this.dataPort = Integer.parseInt(conf.getOrDefault(Topology.WORKER_DATA_PORT, Topology.DEFAULT_DATA_PORT));
		this.clientThreadCount = Integer.parseInt(conf.getOrDefault(Topology.CLIENT_THREAD_COUNT, "1"));
	}

	/**
	 * Start {@link Router}
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public void start() throws Exception {
		while (columbus.getWorkerCount() < workerCount) {
			Thread.sleep(2000);
			logger.info("Waiting for worker discovery");
		}
		
		pool = Executors.newFixedThreadPool(1 + clientThreadCount);
		server = (NetworkServer<E>) Class.forName(networkServerClass).newInstance();
		server.setBindAddress(bindAddress);
		server.setDataPort(dataPort);
		server.setClassOf(classOf);
		server.setRouter(this);
		server.setColumbus(getColumbus());
		pool.submit(() -> {
			try {
				server.start();
			} catch (Exception e) {
				throw new RuntimeException("TCP Transport Server failed", e);
			}
		});

		networkTranmissionDisruptor = new Disruptor<E>(factory, 1024 * 8, pool, ProducerType.MULTI,
				new YieldingWaitStrategy());
		clients = new ArrayList<>(clientThreadCount);
		for (int i = 0; i < clientThreadCount; i++) {
			NetworkClient<E> client = (NetworkClient<E>) Class.forName(networkClientClass).newInstance();
			client.setColumbus(getColumbus());
			client.setClientThreads(clientThreadCount);
			client.setClientThreadId(i);
			client.start();
			clients.add(client);
		}
		networkTranmissionDisruptor.handleEventsWith(clients.toArray(new NetworkClient[1]));
		networkTranmissionDisruptor.start();
	}

	/**
	 * Stop {@link Router}
	 * 
	 * @throws Exception
	 */
	public void stop() throws Exception {
		server.stop();
		networkTranmissionDisruptor.shutdown();
		pool.shutdownNow();
	}

	/**
	 * Direct local routing
	 * 
	 * @param nextBoltName
	 * @param taskId
	 * @param tuple
	 */
	public void directLocalRouteEvent(String nextBoltName, int taskId, E tuple) {
		executorMap.get(nextBoltName).process(taskId, tuple);
	}

	/**
	 * {@link Router} method called for {@link Tuple} routing. This method uses
	 * {@link ROUTING_TYPE} for the nextProcessorId to fetch the
	 * {@link BoltExecutor} and get the taskId to route the message to.
	 * 
	 * @param tuple
	 */
	public void routeTuple(E tuple) {
		BoltExecutor<E> nextBolt = executorMap.get(tuple.getNextBoltId());
		if (nextBolt == null) {
			// drop this event
			System.err.println("Next bolt null, droping event:" + tuple);
			return;
		}
		int taskId = -1;
		int workerCount = columbus.getWorkerCount();

		// normalize parallelism
		int totalParallelism = nextBolt.getParallelism(); // get
															// local
															// parallelism
		totalParallelism = workerCount * totalParallelism;
		switch (nextBolt.getTemplateBoltInstance().getRoutingType()) {
		case GROUPBY:
			Object key = tuple.getGroupByKey();
			if (key != null) {
				taskId = Math.abs(MurmurHash.hash32(key.toString()) % totalParallelism);
			} else {
				System.err.println("Droping event, missing field group by:" + tuple.getNextBoltId());
				// discard event
			}
			break;
		case SHUFFLE:
			// adding local only shuffling to reduce network traffic
			taskId = nextBolt.getParallelism() * columbus.getSelfWorkerId()
					+ Math.abs((int) (tuple.getTupleId() % nextBolt.getParallelism()));
			break;
		}

		// check if this taskId is local to this worker
		routeToTaskId(tuple, nextBolt, taskId);
	}

	/**
	 * Used to either local or network route an event based on worker id.
	 * 
	 * @param tuple
	 * @param nextBolt
	 * @param taskId
	 */
	public void routeToTaskId(E tuple, BoltExecutor<E> nextBolt, int taskId) {
		if (nextBolt == null) {
			nextBolt = executorMap.get(tuple.getNextBoltId());
		}
		int destinationWorker = 0;
		if (taskId >= nextBolt.getParallelism()) {
			destinationWorker = taskId / nextBolt.getParallelism();
		}

		if (destinationWorker == columbus.getSelfWorkerId()) {
			nextBolt.process(taskId, tuple);
		} else {
			// logger.info("Network routing");
			tuple.setDestinationTaskId(taskId);
			tuple.setDestinationWorkerId(destinationWorker);
			networkTranmissionDisruptor.publishEvent(translator, tuple);
		}
	}

	/**
	 * Proxy method to get self worker id
	 * 
	 * @return self worker id
	 */
	public int getSelfWorkerId() {
		return columbus.getSelfWorkerId();
	}

	/**
	 * @return columbus
	 */
	public Columbus getColumbus() {
		return columbus;
	}

}
