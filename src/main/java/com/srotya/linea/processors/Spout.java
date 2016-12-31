/**
 * Copyright 2016 Ambud Sharma
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

import com.srotya.linea.Event;
import com.srotya.linea.disruptor.ROUTING_TYPE;
import com.srotya.linea.utils.Constants;

/**
 * Spout is a type of {@link Bolt} that generates data that is processed
 * by the rest of the topology.
 * 
 * @author ambud
 */
public abstract class Spout implements Bolt {

	private static final long serialVersionUID = 1L;

	@Override
	public void process(Event event) {
		Object object = event.getHeaders().get(Constants.FIELD_GROUPBY_ROUTING_KEY);
		if(object!=null) {
			Boolean type = (Boolean) event.getHeaders().get(Constants.FIELD_EVENT_TYPE);
			if(type) {
				ack((Long)object);
			}else {
				fail((Long)object);
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
	public ROUTING_TYPE getRoutingType() {
		return ROUTING_TYPE.GROUPBY;
	}

}
