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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.srotya.linea.clustering.Columbus;
import com.srotya.linea.clustering.columbus.SingletonNodeClusterKeeper;
import com.srotya.linea.example.simple.Event;
import com.srotya.linea.example.simple.EventFactory;
import com.srotya.linea.example.simple.EventTranslator;
import com.srotya.linea.example.simple.PrinterBolt;
import com.srotya.linea.example.simple.TestSpout;

/**
 * Simple test topology to validate how Linea will launch and run pipelines and
 * acking.
 * 
 * @author ambud
 */
public class TestSimpleTopology {

	public static Map<Integer, AtomicBoolean> processed = new HashMap<>();

	@Test
	public void testTopologyEndToEnd() throws Exception {
		Map<String, String> conf = new HashMap<>();
		conf.put(Columbus.KEEPER_CLASS_FQCN, SingletonNodeClusterKeeper.class.getName());
		conf.put(Topology.WORKER_ID, "0");
		conf.put(Topology.WORKER_COUNT, "1");
		conf.put(Topology.WORKER_DATA_PORT, "3422");
		int parallelism = 1;
		conf.put(Topology.ACKER_PARALLELISM, String.valueOf(parallelism * 1));
		Topology<Event> topology = new Topology<Event>(conf, new EventFactory(), new EventTranslator(), Event.class);
		processed = new HashMap<>();
		topology = topology.addSpout(new TestSpout(1 * 1_000_000), parallelism * 1)
				.addBolt(new PrinterBolt(), parallelism).start();
		while (!processed.get(0).get()) {
			Thread.sleep(100);
		}
		topology.stop();
	}

//	@Test
//	public void testMultiNodeTopologyEndToEnd() throws Exception {
//		Map<String, String> worker1 = new HashMap<>();
//		worker1.put(Columbus.KEEPER_CLASS_FQCN, SingletonNodeClusterKeeper.class.getName());
//		worker1.put(Topology.WORKER_ID, "0");
//		worker1.put(Topology.WORKER_COUNT, "2");
//		worker1.put(Topology.WORKER_DATA_PORT, "3422");
//		int parallelism = 1;
//		worker1.put(Topology.ACKER_PARALLELISM, String.valueOf(parallelism * 1));
//		Topology<Event> worker1Topology = new Topology<Event>(worker1, new EventFactory(), new EventTranslator(),
//				Event.class);
//		worker1Topology.addSpout(new TestSpout(10_000), parallelism * 1).addBolt(new PrinterBolt(), parallelism);
//
//		Map<String, String> worker2 = new HashMap<>();
//		worker2.put(Columbus.KEEPER_CLASS_FQCN, SingletonNodeClusterKeeper.class.getName());
//		worker2.put(Topology.WORKER_ID, "1");
//		worker2.put(Topology.WORKER_COUNT, "2");
//		worker2.put(Topology.WORKER_DATA_PORT, "3425");
//		worker2.put(Topology.ACKER_PARALLELISM, String.valueOf(parallelism * 1));
//		Topology<Event> worker2Topology = new Topology<Event>(worker2, new EventFactory(), new EventTranslator(),
//				Event.class);
//		processed = new HashMap<>();
//		processed.put(0, new AtomicBoolean());
//		processed.put(1, new AtomicBoolean());
//		worker2Topology.addSpout(new TestSpout(10_000), parallelism * 1).addBolt(new PrinterBolt(), parallelism);
//		Executors.newSingleThreadExecutor(new ThreadFactory() {
//
//			@Override
//			public Thread newThread(Runnable r) {
//				Thread th = new Thread(r);
//				th.setDaemon(true);
//				return th;
//			}
//		}).submit(() -> worker2Topology.start());
//		worker1Topology.start();
//		Thread.sleep(5000);
//		while (!processed.get(0).get() && !processed.get(1).get()) {
//			System.out.println("Waiting....");
//			Thread.sleep(1000);
//		}
//	}
}
