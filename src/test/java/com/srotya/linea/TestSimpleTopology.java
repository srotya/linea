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

import com.srotya.linea.Topology;
import com.srotya.linea.clustering.Columbus;
import com.srotya.linea.clustering.columbus.SingleNodeClusterKeeper;
import com.srotya.linea.example.simple.Event;
import com.srotya.linea.example.simple.EventFactory;
import com.srotya.linea.example.simple.EventTranslator;
import com.srotya.linea.example.simple.PrinterBolt;
import com.srotya.linea.example.simple.TestSpout;
import com.srotya.linea.example.simple.TransformBolt;

/**
 * Simple test topology to validate how Linea will launch and run pipelines and
 * acking.
 * 
 * @author ambud
 */
public class TestSimpleTopology {
	
	public static AtomicBoolean processed = new AtomicBoolean(false);

	@Test
	public void testTopologyEndToEnd() throws Exception {
		Map<String, String> conf = new HashMap<>();
		conf.put(Columbus.KEEPER_CLASS_FQCN, SingleNodeClusterKeeper.class.getName());
		conf.put(Topology.WORKER_ID, "0");
		conf.put(Topology.WORKER_COUNT, "1");
		conf.put(Topology.WORKER_DATA_PORT, "3422");
		int parallelism = 1;
		conf.put(Topology.ACKER_PARALLELISM, String.valueOf(parallelism * 1));
		Topology<Event> topology = new Topology<Event>(conf, new EventFactory(), new EventTranslator(), Event.class);
		topology = topology.addSpout(new TestSpout(1 * 100_000), parallelism * 1)
				.addBolt(new TransformBolt(), parallelism * 1).addBolt(new PrinterBolt(), parallelism).start();
		while(!processed.get()) {
			Thread.sleep(100);
		}
		topology.stop();
	}
}
