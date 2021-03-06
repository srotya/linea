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
package com.srotya.linea.example.simple;

import java.text.NumberFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.srotya.linea.Collector;
import com.srotya.linea.TestSimpleTopology;
import com.srotya.linea.processors.Spout;

/**
 * @author ambud
 */
public class TestSpout extends Spout<Event> {

	private static final long serialVersionUID = 1L;
	private transient Collector<Event> collector;
	private transient Set<Long> emittedEvents;
	private transient int taskId;
	private transient AtomicBoolean processed;
	private int c;
	private int messageCount;

	public TestSpout(int messageCount) {
		this.messageCount = messageCount;
	}

	@Override
	public void configure(Map<String, String> conf, int taskId, Collector<Event> collector) {
		this.taskId = taskId;
		this.processed = new AtomicBoolean(false);
		this.collector = collector;
		TestSimpleTopology.processed.put(taskId, new AtomicBoolean());
		emittedEvents = Collections.synchronizedSet(new HashSet<>());
	}

	@Override
	public String getSpoutName() {
		return "testSpout";
	}

	@Override
	public void ready() {
		System.out.println("Running spout:" + taskId);
		long timestamp = System.currentTimeMillis();
		for (int i = 0; i < messageCount; i++) {
			if (Thread.currentThread().isInterrupted()) {
				break;
			}
			Event event = (Event) collector.getFactory().buildTuple(taskId + "_" + i);
			event.setGroupByKey("host" + i);
			emittedEvents.add(event.getTupleId());
			collector.spoutEmit("printerBolt", event);
			if (i % 100000 == 0) {
				System.err.println("Produced " + i + " events:" + taskId);
			}
		}
		processed.set(true);
		timestamp = System.currentTimeMillis() - timestamp;
		System.out.println("Emitted all events in:" + timestamp / 1000 + " seconds");
		timestamp = System.currentTimeMillis();
		while (true) {
			if (emittedEvents.size() == 0) {
				NumberFormat formatter = NumberFormat.getInstance();
				System.out.println(
						"Completed processing " + formatter.format(messageCount) + " events" + "\ttaskid:" + taskId);
				timestamp = System.currentTimeMillis() - timestamp;
				System.out.println("Add additional:" + timestamp / 1000 + " seconds for buffer to be processed");
				TestSimpleTopology.processed.get(taskId).set(true);
				try {
					Thread.sleep(5000);
					System.exit(0);
				} catch (InterruptedException e) {
					break;
				}
			} else {
				System.out.println(c + "\tDropped data:" + emittedEvents.size() + "\ttaskid:" + taskId);
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void ack(Long eventId) {
		// boolean removed = false;
		emittedEvents.remove(eventId);
		c++;
	}

	@Override
	public void fail(Long eventId) {
		System.out.println("Spout failing event:" + eventId);
	}

}
