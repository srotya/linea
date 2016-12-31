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
package com.srotya.linea.clustering;

import java.util.Map;

/**
 * Cluster Keeper is responsible for mutual discovery of worker nodes in a
 * topology. The implementations for a Keeper can be backed by the different
 * data-stores or other discovery algorithms. <br>
 * <br>
 * Keeper is called by Columbus to perform discovery operations.
 * 
 * @author ambud
 */
public interface ClusterKeeper {

	/**
	 * Initialize the Keeper (connect to data store)
	 * 
	 * @param conf
	 * @throws Exception
	 */
	public void init(Map<String, String> conf) throws Exception;

	/**
	 * Register this worker so that other workers can discover it
	 * 
	 * @param selfWorkerId
	 * @param entry
	 * @return
	 * @throws Exception
	 */
	public int registerWorker(int selfWorkerId, WorkerEntry entry) throws Exception;

	/**
	 * Poll workers and get their worker IDs as well as port info
	 * 
	 * @return worker map
	 * @throws Exception
	 */
	public Map<Integer, WorkerEntry> pollWorkers() throws Exception;

}
