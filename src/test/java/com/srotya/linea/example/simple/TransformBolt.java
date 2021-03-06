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

import java.util.Map;

import com.srotya.linea.Collector;
import com.srotya.linea.disruptor.ROUTING_TYPE;
import com.srotya.linea.processors.Bolt;

/**
 * @author ambud
 */
public class TransformBolt implements Bolt<Event> {

	private static final long serialVersionUID = 1L;
	private transient Collector<Event> collector;

	@Override
	public void configure(Map<String, String> conf, int instanceId, Collector<Event> collector) {
		this.collector = collector;
	}

	@Override
	public void ready() {
	}

	@Override
	public void process(Event event) {
		Event buildEvent = collector.getFactory().buildTuple();
		buildEvent.getHeaders().put("fieldtransform", 2231);
		collector.emit("printerBolt", buildEvent, event);
		collector.ack(event);
	}

	@Override
	public ROUTING_TYPE getRoutingType() {
		return ROUTING_TYPE.SHUFFLE;
	}

	@Override
	public String getBoltName() {
		return "transformBolt";
	}

	@Override
	public int tickTupleFrequency() {
		return 0;
	}

}
