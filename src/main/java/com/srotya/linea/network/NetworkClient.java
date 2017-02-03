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
package com.srotya.linea.network;

import com.lmax.disruptor.EventHandler;
import com.srotya.linea.Tuple;
import com.srotya.linea.clustering.Columbus;

/**
 * @author ambud
 *
 * @param <E>
 */
public abstract class NetworkClient<E extends Tuple> implements EventHandler<E> {
	
	private Columbus columbus;
	private int clientThreads;
	private int clientThreadId;
	
	public NetworkClient() {
	}

	public abstract void start() throws Exception;

	/**
	 * @return the columbus
	 */
	public Columbus getColumbus() {
		return columbus;
	}

	/**
	 * @param columbus the columbus to set
	 */
	public void setColumbus(Columbus columbus) {
		this.columbus = columbus;
	}

	/**
	 * @return the clientThreads
	 */
	public int getClientThreads() {
		return clientThreads;
	}

	/**
	 * @param clientThreads the clientThreads to set
	 */
	public void setClientThreads(int clientThreads) {
		this.clientThreads = clientThreads;
	}

	/**
	 * @return the clientThreadId
	 */
	public int getClientThreadId() {
		return clientThreadId;
	}

	/**
	 * @param clientThreadId the clientThreadId to set
	 */
	public void setClientThreadId(int clientThreadId) {
		this.clientThreadId = clientThreadId;
	}
	
}
