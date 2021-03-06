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
package com.srotya.linea.clustering;

import java.net.InetAddress;

import com.srotya.linea.utils.NetUtils;

/**
 * Entry for the {@link Columbus} worker map.
 * 
 * @author ambud
 */
public class WorkerEntry implements Comparable<WorkerEntry> {

	private int workerId;
	private InetAddress workerAddress;
	private long lastContactTimestamp;
	private int dataPort;
	private boolean qourumEstablished;

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
			return workerAddress.equals(param.workerAddress) && dataPort == param.dataPort;
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

	/**
	 * @return the workerId
	 */
	public int getWorkerId() {
		return workerId;
	}

	/**
	 * @param workerId
	 *            the workerId to set
	 */
	public void setWorkerId(int workerId) {
		this.workerId = workerId;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "WorkerEntry [workerId=" + workerId + ", workerAddress=" + workerAddress + ", lastContactTimestamp="
				+ lastContactTimestamp + ", dataPort=" + dataPort + ", qourumEstablished=" + qourumEstablished + "]";
	}

}
