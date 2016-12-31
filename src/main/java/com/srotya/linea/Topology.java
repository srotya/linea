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
package com.srotya.linea;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.srotya.linea.clustering.Columbus;
import com.srotya.linea.network.Router;
import com.srotya.linea.processors.Bolt;
import com.srotya.linea.processors.BoltExecutor;
import com.srotya.linea.processors.DisruptorUnifiedFactory;
import com.srotya.linea.processors.Spout;
import com.srotya.linea.tolerance.AckerBolt;

/**
 * Runnable representation of the Topology, this class is also used to
 * orchestrate the build and wiring of topology. <br>
 * <br>
 * Class provides a fluent API to allow ease of development.
 * 
 * @author ambud
 */
public class Topology {

	public static final String DEFAULT_ACKER_PARALLELISM = "1";
	public static final String DEFAULT_DATA_PORT = "5000";
	public static final String ACKER_PARALLELISM = "acker.parallelism";
	public static final String WORKER_COUNT = "worker.count";
	public static final String WORKER_ID = "worker.id";
	private Map<String, String> conf;
	private DisruptorUnifiedFactory factory;
	private Map<String, BoltExecutor> executorMap;
	private Router router;
	private Columbus columbus;
	private int workerCount;
	private int ackerCount;
	private ExecutorService backgroundServices;
	public static final String WORKER_DATA_PORT = "worker.data.port";
	public static final String WORKER_BIND_ADDRESS = "worker.data.bindAddress";
	public static final String DEFAULT_BIND_ADDRESS = "localhost";

	/**
	 * Constructor with configuration properties
	 * 
	 * @param conf
	 * @throws Exception
	 */
	public Topology(Map<String, String> conf) throws Exception {
		this.conf = conf;
		init();
	}

	public Topology(Properties props) throws Exception {
		conf = new HashMap<>();
		for (Entry<Object, Object> entry : props.entrySet()) {
			conf.put(entry.getKey().toString(), entry.getValue().toString());
		}
		init();
	}

	/**
	 * Initialize the Topology Builder
	 * 
	 * @throws Exception
	 */
	protected void init() throws Exception {
		factory = new DisruptorUnifiedFactory();
		executorMap = new HashMap<>();
		ackerCount = Integer.parseInt(conf.getOrDefault(ACKER_PARALLELISM, DEFAULT_ACKER_PARALLELISM));
		columbus = new Columbus(conf);
		workerCount = Integer.parseInt(conf.getOrDefault(WORKER_COUNT, DEFAULT_ACKER_PARALLELISM));
		router = new Router(factory, columbus, workerCount, executorMap, conf);
		backgroundServices = Executors.newFixedThreadPool(1);
	}

	/**
	 * Add Spout with parallelism
	 * 
	 * @param spout
	 * @param parallelism
	 * @return topology builder
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public Topology addSpout(Spout spout, int parallelism) throws IOException, ClassNotFoundException {
		return addBolt(spout, parallelism);
	}

	/**
	 * Add Bolt with parallelism
	 * 
	 * @param bolt
	 * @param parallelism
	 * @return topology builder
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public Topology addBolt(Bolt bolt, int parallelism) throws IOException, ClassNotFoundException {
		byte[] serializeBoltInstance = BoltExecutor.serializeBoltInstance(bolt);
		BoltExecutor boltExecutor = new BoltExecutor(conf, factory, serializeBoltInstance, columbus, parallelism,
				router);
		executorMap.put(boltExecutor.getTemplateBoltInstance().getBoltName(), boltExecutor);
		return this;
	}

	/**
	 * Start this topology
	 * 
	 * @return topology builder
	 * @throws Exception
	 */
	public Topology start() throws Exception {
		final Topology self = this;
		// attach shutdown hook to gracefully stop topology
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					self.stop();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		// attach acker bolt
		addBolt(new AckerBolt(), ackerCount);
		// start columbus
		backgroundServices.submit(() -> columbus.run());
		// start router
		router.start();
		// start each bolt executor
		for (Entry<String, BoltExecutor> entry : executorMap.entrySet()) {
			entry.getValue().start();
		}
		return this;
	}

	/**
	 * Stop this topology
	 * 
	 * @return topology builder
	 * @throws Exception
	 */
	public Topology stop() throws Exception {
		// stop each bolt executor
		for (Entry<String, BoltExecutor> entry : executorMap.entrySet()) {
			entry.getValue().stop();
		}
		// stop router
		router.stop();
		return this;
	}

	/**
	 * @return factory
	 */
	public DisruptorUnifiedFactory getFactory() {
		return factory;
	}

	/**
	 * @return router
	 */
	public Router getRouter() {
		return router;
	}

}
