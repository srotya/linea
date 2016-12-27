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

import java.io.Serializable;
import java.net.InetAddress;

import com.srotya.linea.utils.NetUtils;

/**
 * @author ambud
 */
public class WorkerEntry implements Serializable, Comparable<WorkerEntry> {

	private static final long serialVersionUID = 1L;
	private InetAddress workerAddress;
	private long lastContactTimestamp;
	private int dataPort;
	private boolean qourumEstablished;

	public WorkerEntry() {
	}

	public WorkerEntry(InetAddress workerAddress, int dataPort, long lastContactTimestamp) {
		this.workerAddress = workerAddress;
		this.dataPort = dataPort;
		this.lastContactTimestamp = lastContactTimestamp;
	}

	/**
	 * @return the workerAddress
	 */
	public InetAddress getWorkerAddress() {
		return workerAddress;
	}

	/**
	 * @param workerAddress
	 *            the workerAddress to set
	 */
	public void setWorkerAddress(InetAddress workerAddress) {
		this.workerAddress = workerAddress;
	}

	/**
	 * @return the lastContactTimestamp
	 */
	public long getLastContactTimestamp() {
		return lastContactTimestamp;
	}

	/**
	 * @param lastContactTimestamp
	 *            the lastContactTimestamp to set
	 */
	public void setLastContactTimestamp(long lastContactTimestamp) {
		this.lastContactTimestamp = lastContactTimestamp;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof WorkerEntry) {
			WorkerEntry param = (WorkerEntry) obj;
			return workerAddress == param.workerAddress;
		}
		return false;
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

	@Override
	public int compareTo(WorkerEntry o) {
		return Integer.compare(NetUtils.stringIPtoInt(getWorkerAddress().getHostAddress()),
				NetUtils.stringIPtoInt(o.getWorkerAddress().getHostAddress()));
	}

	/**
	 * @return the qourumEstablished
	 */
	public boolean isQourumEstablished() {
		return qourumEstablished;
	}

	/**
	 * @param qourumEstablished
	 *            the qourumEstablished to set
	 */
	public void setQourumEstablished(boolean qourumEstablished) {
		this.qourumEstablished = qourumEstablished;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "WorkerEntry [workerAddress=" + workerAddress + ", lastContactTimestamp=" + lastContactTimestamp + "]";
	}

}
