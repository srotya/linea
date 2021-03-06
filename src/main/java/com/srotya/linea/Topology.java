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
package com.srotya.linea;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.srotya.linea.clustering.Columbus;
import com.srotya.linea.disruptor.CopyTranslator;
import com.srotya.linea.network.Router;
import com.srotya.linea.processors.Bolt;
import com.srotya.linea.processors.BoltExecutor;
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
public class Topology<E extends Tuple> {

	public static final String DEFAULT_ACKER_PARALLELISM = "1";
	public static final String DEFAULT_DATA_PORT = "5000";
	public static final String ACKER_PARALLELISM = "acker.parallelism";
	public static final String WORKER_COUNT = "worker.count";
	public static final String WORKER_ID = "worker.id";
	private Map<String, String> conf;
	private TupleFactory<E> factory;
	private Map<String, BoltExecutor<E>> executorMap;
	private CopyTranslator<E> translator;
	private Router<E> router;
	private Columbus columbus;
	private int workerCount;
	private int ackerCount;
	private ExecutorService backgroundServices;
	public static final String WORKER_DATA_PORT = "worker.data.port";
	public static final String WORKER_BIND_ADDRESS = "worker.data.bindAddress";
	public static final String DEFAULT_BIND_ADDRESS = "localhost";
	public static final Object CLIENT_THREAD_COUNT = "client.thread.count";
	public static final String TICK_TUPLE = "_tick";
	private Class<E> classOf;
	public static final String TICK_TUPLE_FREQUENCY = "tick.tuple.frequency";

	/**
	 * Constructor with configuration properties
	 * 
	 * @param conf
	 * @throws Exception
	 */
	public Topology(Map<String, String> conf, TupleFactory<E> factory, CopyTranslator<E> translator, Class<E> classOf)
			throws Exception {
		this.conf = conf;
		this.factory = factory;
		this.translator = translator;
		this.classOf = classOf;
		init();
	}

	/**
	 * Initialize the Topology Builder
	 * 
	 * @throws Exception
	 */
	protected void init() throws Exception {
		executorMap = new LinkedHashMap<>();
		ackerCount = Integer.parseInt(conf.getOrDefault(ACKER_PARALLELISM, DEFAULT_ACKER_PARALLELISM));
		columbus = new Columbus(conf);
		workerCount = Integer.parseInt(conf.getOrDefault(WORKER_COUNT, DEFAULT_ACKER_PARALLELISM));
		router = new Router<E>(classOf, factory, columbus, workerCount, executorMap, conf, translator);
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
	public Topology<E> addSpout(Spout<E> spout, int parallelism) throws IOException, ClassNotFoundException {
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
	public Topology<E> addBolt(Bolt<E> bolt, int parallelism) throws IOException, ClassNotFoundException {
		byte[] serializeBoltInstance = serializeBoltInstance(bolt);
		BoltExecutor<E> boltExecutor = new BoltExecutor<E>(conf, factory, serializeBoltInstance, columbus, parallelism,
				router, translator);
		executorMap.put(boltExecutor.getTemplateBoltInstance().getBoltName(), boltExecutor);
		return this;
	}

	/**
	 * Deserialize bolt instance from the byte array
	 * 
	 * @param processorObject
	 * @return bolt instance
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public Bolt<E> deserializeBoltInstance(byte[] processorObject) throws IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(processorObject));
		@SuppressWarnings("unchecked")
		Bolt<E> processor = (Bolt<E>) ois.readObject();
		ois.close();
		return processor;
	}

	/**
	 * Serialize {@link Bolt} instance to byte array
	 * 
	 * @param boltInstance
	 * @return byte array
	 * @throws IOException
	 */
	public byte[] serializeBoltInstance(Bolt<E> boltInstance) throws IOException {
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		ObjectOutputStream ois = new ObjectOutputStream(stream);
		ois.writeObject(boltInstance);
		ois.close();
		return stream.toByteArray();
	}

	/**
	 * Start this topology
	 * 
	 * @return topology builder
	 * @throws Exception
	 */
	public Topology<E> start() throws Exception {
		final Topology<E> self = this;
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
		addBolt(new AckerBolt<E>(), ackerCount);
		// start columbus
		backgroundServices.submit(() -> columbus.run());
		// start router
		router.start();
		// start each bolt executor
		for (Entry<String, BoltExecutor<E>> entry : executorMap.entrySet()) {
			if (!(entry.getValue().getTemplateBoltInstance() instanceof Spout)) {
				entry.getValue().start();
			}
		}
		for (Entry<String, BoltExecutor<E>> entry : executorMap.entrySet()) {
			if (entry.getValue().getTemplateBoltInstance() instanceof Spout) {
				entry.getValue().start();
			}
		}
		return this;
	}

	/**
	 * Stop this topology
	 * 
	 * @return topology builder
	 * @throws Exception
	 */
	public Topology<E> stop() throws Exception {
		// stop each bolt executor
		for (Entry<String, BoltExecutor<E>> entry : executorMap.entrySet()) {
			entry.getValue().stop();
		}
		// stop router
		router.stop();
		return this;
	}

}
