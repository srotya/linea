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
package com.srotya.linea.processors;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.srotya.linea.Collector;
import com.srotya.linea.Topology;
import com.srotya.linea.Tuple;
import com.srotya.linea.TupleFactory;
import com.srotya.linea.clustering.Columbus;
import com.srotya.linea.disruptor.CopyTranslator;
import com.srotya.linea.network.Router;

/**
 * {@link Bolt} Executor is wrapper that instantiates and executes bolt code.
 * 
 * @author ambud
 */
public class BoltExecutor<E extends Tuple> {

	private static final Logger logger = Logger.getLogger(BoltExecutor.class.getName());
	private ExecutorService es;
	private Bolt<E> templateBoltInstance;
	private Map<Integer, BoltExecutorWrapper<E>> taskProcessorMap;
	private CopyTranslator<E> copyTranslator;
	private int parallelism;
	private Columbus columbus;
	private byte[] serializedBoltInstance;
	private Map<String, String> conf;
	private Router<E> router;
	private TupleFactory<E> factory;

	/**
	 * @param conf
	 * @param factory
	 * @param serializedBoltInstance
	 * @param columbus
	 * @param parallelism
	 * @param router
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public BoltExecutor(Map<String, String> conf, TupleFactory<E> factory, byte[] serializedBoltInstance,
			Columbus columbus, int parallelism, Router<E> router, CopyTranslator<E> copyTranslator)
			throws IOException, ClassNotFoundException {
		this.conf = conf;
		this.factory = factory;
		this.serializedBoltInstance = serializedBoltInstance;
		this.columbus = columbus;
		this.parallelism = parallelism;
		this.router = router;
		this.taskProcessorMap = new HashMap<>();
		this.templateBoltInstance = deserializeBoltInstance(serializedBoltInstance);
		this.es = Executors.newFixedThreadPool(parallelism * 2);
		this.copyTranslator = copyTranslator;
	}

	/**
	 * Start method for this bolt executor
	 */
	public void start() {
		/**
		 * First worker 0*4+0 = 0 0*4+1 = 1 0*4+2 = 2 = 3 Second worker 1*4+0 =
		 * 4 1*4+1 = 5 1*4+2 = 6
		 *
		 * Third worker 2*3+0 = 6
		 * 
		 * Or First worker 0*2+0 = 0 Second worker 1*2+0 = 2
		 */
		// es.submit(() -> {
		// });
		try {
			for (int i = 0; i < parallelism; i++) {
				int taskId = columbus.getSelfWorkerId() * parallelism + i;
				Bolt<E> object = deserializeBoltInstance(serializedBoltInstance);
				object.configure(conf, taskId,
						new Collector<E>(factory, router, object.getBoltName(), taskId, parallelism));
				taskProcessorMap.put(taskId, new BoltExecutorWrapper<E>(factory, es, object, copyTranslator));
			}
			for (Entry<Integer, BoltExecutorWrapper<E>> entry : taskProcessorMap.entrySet()) {
				entry.getValue().start();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Stop method for this bolt executor
	 * 
	 * @throws InterruptedException
	 */
	public void stop() throws InterruptedException {
		for (Entry<Integer, BoltExecutorWrapper<E>> entry : taskProcessorMap.entrySet()) {
			entry.getValue().stop();
		}
		es.shutdownNow();
		es.awaitTermination(1000, TimeUnit.MILLISECONDS);
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
	 * Method called by Router
	 * 
	 * @param taskId
	 * @param tuple
	 */
	public void process(int taskId, E tuple) {
		BoltExecutorWrapper<E> wrapper = taskProcessorMap.get(taskId);
		if (wrapper != null) {
			wrapper.getBuffer().publishEvent(copyTranslator, tuple);
		} else {
			logger.severe("Executor not found for:" + taskId + "\t" + columbus.getSelfWorkerId() + "\t"
					+ taskProcessorMap + "\t" + tuple);
		}
	}

	/**
	 * @return templatedBoltInstance
	 */
	public Bolt<E> getTemplateBoltInstance() {
		return templateBoltInstance;
	}

	/**
	 * @return parallelism
	 */
	public int getParallelism() {
		return parallelism;
	}

	/**
	 * @return the taskProcessorMap
	 */
	protected Map<Integer, BoltExecutorWrapper<E>> getTaskProcessorMap() {
		return taskProcessorMap;
	}

	/**
	 * @return the es
	 */
	protected ExecutorService getEs() {
		return es;
	}

	/**
	 * Bolt Executor Wrapper
	 * 
	 * @author ambud
	 */
	public static class BoltExecutorWrapper<E extends Tuple> implements EventHandler<E> {

		private Bolt<E> bolt;
		private Disruptor<E> disruptor;
		private RingBuffer<E> buffer;
		private ExecutorService pool;
		private TupleFactory<E> factory;
		private volatile boolean flag;
		private CopyTranslator<E> copyTranslator;

		@SuppressWarnings("unchecked")
		public BoltExecutorWrapper(TupleFactory<E> factory, ExecutorService pool, Bolt<E> processor,
				CopyTranslator<E> copyTranslator) {
			this.copyTranslator = copyTranslator;
			this.flag = true;
			this.factory = factory;
			this.pool = pool;
			this.bolt = processor;
			disruptor = new Disruptor<>(factory, 1024 * 8, pool, ProducerType.MULTI, new YieldingWaitStrategy());
			disruptor.handleEventsWith(this);
		}

		/**
		 * Start {@link BoltExecutorWrapper}
		 */
		public void start() {
			buffer = disruptor.start();
			pool.submit(() -> {
				try {
					// wait 3 seconds before launching ready methods
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					return;
				}
				bolt.ready();
				if (bolt.tickTupleFrequency() > 0) {
					while (flag) {
						E tickTuple = factory.buildTuple();
						tickTuple.setComponentName(Topology.TICK_TUPLE);
						buffer.publishEvent(copyTranslator, tickTuple);
						try {
							Thread.sleep(bolt.tickTupleFrequency());
						} catch (InterruptedException e) {
							e.printStackTrace();
							break;
						}
					}
				}
			});
		}

		/**
		 * Stop {@link BoltExecutorWrapper}
		 */
		public void stop() {
			flag = true;
			disruptor.shutdown();
		}

		@Override
		public void onEvent(E event, long arg1, boolean arg2) throws Exception {
			bolt.process(event);
		}

		/**
		 * @return buffer
		 */
		public RingBuffer<E> getBuffer() {
			return buffer;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "ProcessorWrapper [processor=" + bolt.getBoltName() + "]";
		}
	}

}
