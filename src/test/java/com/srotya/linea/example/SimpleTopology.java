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
package com.srotya.linea.example;

import java.util.HashMap;
import java.util.Map;

import com.srotya.linea.Event;
import com.srotya.linea.Topology;
import com.srotya.linea.TupleFactory;
import com.srotya.linea.disruptor.CopyTranslator;

/**
 * Simple test topology to validate how Linea will launch and run pipelines and
 * acking.
 * 
 * Fixed bugs with copy translator causing issues in acking.
 * 
 * @author ambud
 */
public class SimpleTopology {

	public static void main(String[] args) throws Exception {
		Map<String, String> conf = new HashMap<>();
		conf.put(Topology.WORKER_ID, args[0]);
		conf.put(Topology.WORKER_COUNT, args[1]);
		conf.put(Topology.WORKER_DATA_PORT, args[2]);
		conf.put(Topology.ACKER_PARALLELISM, "2");
		Topology<Event> builder = new Topology<Event>(conf, new EventFactory(), new EventTranslator(), Event.class);
		builder = builder.addSpout(new TestSpout(10_000_000), 2).addBolt(new PrinterBolt(), 2).start();
	}

	public static class EventTranslator extends CopyTranslator<Event> {

		@Override
		public void translateTo(Event outputEvent, long sequence, Event inputEvent) {
			outputEvent.getHeaders().clear();
			outputEvent.getHeaders().putAll(inputEvent.getHeaders());
			outputEvent.getSourceIds().clear();
			outputEvent.getSourceIds().addAll(inputEvent.getSourceIds());
			outputEvent.setEventId(inputEvent.getEventId());
			outputEvent.setSourceWorkerId(inputEvent.getSourceWorkerId());
			outputEvent.setOriginEventId(inputEvent.getOriginEventId());
			outputEvent.setGroupByKey(inputEvent.getGroupByKey());
			outputEvent.setGroupByValue(inputEvent.getGroupByValue());
			outputEvent.setNextBoltId(inputEvent.getNextBoltId());
			outputEvent.setDestinationTaskId(inputEvent.getDestinationTaskId());
			outputEvent.setTaskId(inputEvent.getTaskId());
			outputEvent.setDestinationWorkerId(inputEvent.getDestinationWorkerId());
			outputEvent.setComponentName(inputEvent.getComponentName());
			outputEvent.setAck(inputEvent.isAck());
		}

	}

	public static class EventFactory implements TupleFactory<Event> {

		@Override
		public Event newInstance() {
			return new Event();
		}

		@Override
		public Event buildEvent() {
			return new Event();
		}

		@Override
		public Event buildEvent(String eventId) {
			return new Event(eventId);
		}

	}
}
