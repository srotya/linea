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

import com.srotya.linea.Tuple;
import com.srotya.linea.disruptor.ROUTING_TYPE;

/**
 * Spout is a type of {@link Bolt} that generates data that is processed
 * by the rest of the topology.
 * 
 * @author ambud
 */
public abstract class Spout<E extends Tuple> implements Bolt<E> {

	private static final long serialVersionUID = 1L;

	@Override
	public void process(E event) {
		Object tupleId = event.getGroupByKey();
		if(tupleId!=null) {
			boolean type = event.isAck();
			if(type) {
				ack((Long)tupleId);
			}else {
				fail((Long)tupleId);
			}
		}
	}
	
	/**
	 * Marking eventId as processed
	 * @param eventId
	 */
	public abstract void ack(Long eventId);
	
	/**
	 * Marking eventId as failed
	 * @param eventId
	 */
	public abstract void fail(Long eventId);
	
	@Override
	public final String getBoltName() {
		return "Spout"+getSpoutName();
	}
	
	public abstract String getSpoutName();
	
	@Override
	public ROUTING_TYPE getRoutingType() {
		return ROUTING_TYPE.GROUPBY;
	}
	
	@Override
	public int tickTupleFrequency() {
		return 0;
	}

}
