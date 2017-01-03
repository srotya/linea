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

import java.util.Map;

import com.srotya.linea.Event;
import com.srotya.linea.disruptor.ROUTING_TYPE;
import com.srotya.linea.processors.Bolt;
import com.srotya.linea.tolerance.Collector;

/**
 * @author ambud
 */
public class TransformBolt implements Bolt {

	private static final long serialVersionUID = 1L;
	private transient Collector collector;

	@Override
	public void configure(Map<String, String> conf, int instanceId, Collector collector) {
		this.collector = collector;
	}

	@Override
	public void ready() {
	}

	@Override
	public void process(Event event) {
		Map<String, Object> headers = event.getHeaders();
		headers.put("fieldtransform", 2231);
		collector.emit("printerBolt", headers, event);
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

}
