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
package com.srotya.linea.example.batch;

import java.util.HashMap;
import java.util.Map;

import com.srotya.linea.Topology;

/**
 * Simple test topology to validate how Linea will launch and run pipelines and
 * acking.
 * 
 * @author ambud
 */
public class BatchTopology {

	public static void main(String[] args) throws Exception {
		Map<String, String> conf = new HashMap<>();
		conf.put(Topology.WORKER_ID, args[0]);
		conf.put(Topology.WORKER_COUNT, args[1]);
		conf.put(Topology.WORKER_DATA_PORT, args[2]);
		int parallelism = 1;
		conf.put(Topology.ACKER_PARALLELISM, String.valueOf(parallelism * 2));
		Topology<BatchEvent> builder = new Topology<BatchEvent>(conf, new EventFactory(), new EventTranslator(),
				BatchEvent.class);
		builder = builder.addSpout(new TestBatchSpout(1_000_000_000L), parallelism * 2)
				.addBolt(new TransformBolt(), parallelism).addBolt(new PrinterBolt(), parallelism).start();
	}
}
