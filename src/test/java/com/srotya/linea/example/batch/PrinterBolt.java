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

import java.util.Map;

import com.srotya.linea.Collector;
import com.srotya.linea.disruptor.ROUTING_TYPE;
import com.srotya.linea.processors.Bolt;

/**
 * @author ambud
 */
public class PrinterBolt implements Bolt<BatchEvent> {

	private static final long serialVersionUID = 1L;
	private transient Collector<BatchEvent> collector;
	@SuppressWarnings("unused")
	private transient int taskId;

	@Override
	public void configure(Map<String, String> conf, int taskId, Collector<BatchEvent> collector) {
		this.taskId = taskId;
		this.collector = collector;

	}

	@Override
	public void process(BatchEvent event) {
		if(event.getBatch().size()!=100) {
			System.out.println("Print event:" + event);
		}
		collector.ack(event);
	}

	@Override
	public ROUTING_TYPE getRoutingType() {
		return ROUTING_TYPE.GROUPBY;
	}

	@Override
	public String getBoltName() {
		return "printerBolt";
	}

	@Override
	public void ready() {
	}

}