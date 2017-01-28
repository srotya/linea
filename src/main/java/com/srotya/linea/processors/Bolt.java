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

import java.io.Serializable;
import java.util.Map;

import com.srotya.linea.Collector;
import com.srotya.linea.Tuple;
import com.srotya.linea.disruptor.ROUTING_TYPE;

/**
 * Bolt is custom user defined code for processing {@link Tuple}s. <br>
 * <br>
 * Bolt interface should be implemented by user-code.
 * 
 * @author ambud
 */
public interface Bolt<E extends Tuple> extends Serializable {

	/**
	 * Configure method for initializing the bolt
	 * 
	 * @param conf
	 * @param instanceId
	 * @param collector
	 */
	public void configure(Map<String, String> conf, int instanceId, Collector<E> collector);

	/**
	 * Method asynchronously called just before tuple are started and can be
	 * used for long running background operation.
	 */
	public void ready();

	/**
	 * Method called on each {@link Tuple}
	 * 
	 * @param tuple
	 */
	public void process(E tuple);

	/**
	 * Type of routing to this bolt i.e. Events processed by this bolt
	 * 
	 * @return
	 */
	public ROUTING_TYPE getRoutingType();

	/**
	 * Name of the bolt
	 * 
	 * @return name of bolt
	 */
	public String getBoltName();

}
