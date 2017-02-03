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

import com.srotya.linea.Tuple;
import com.srotya.linea.clustering.Columbus;

/**
 * @author ambud
 *
 * @param <E>
 */
public abstract class NetworkServer<E extends Tuple> {

	private Class<E> classOf;
	private Router<E> router;
	private String bindAddress;
	private int dataPort;
	private Columbus columbus;

	public abstract void start() throws Exception;

	/**
	 * @return the classOf
	 */
	public Class<E> getClassOf() {
		return classOf;
	}

	/**
	 * @param classOf
	 *            the classOf to set
	 */
	public void setClassOf(Class<E> classOf) {
		this.classOf = classOf;
	}

	/**
	 * @return the router
	 */
	public Router<E> getRouter() {
		return router;
	}

	/**
	 * @param router
	 *            the router to set
	 */
	public void setRouter(Router<E> router) {
		this.router = router;
	}

	/**
	 * @return the bindAddress
	 */
	public String getBindAddress() {
		return bindAddress;
	}

	/**
	 * @param bindAddress
	 *            the bindAddress to set
	 */
	public void setBindAddress(String bindAddress) {
		this.bindAddress = bindAddress;
	}

	/**
	 * @return the dataPort
	 */
	public int getDataPort() {
		return dataPort;
	}

	/**
	 * @param dataPort
	 *            the dataPort to set
	 */
	public void setDataPort(int dataPort) {
		this.dataPort = dataPort;
	}

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

	public abstract void stop() throws Exception;

}
