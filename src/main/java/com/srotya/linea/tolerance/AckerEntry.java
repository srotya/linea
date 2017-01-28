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
package com.srotya.linea.tolerance;

import java.io.Serializable;

/**
 * Entry for tracking a tuple
 * 
 * @author ambud
 */
public class AckerEntry implements Serializable {

	private static final long serialVersionUID = 1L;
	private String sourceSpout;
	private int sourceTaskId;
	private long createTime;
	private long value;

	public AckerEntry() {
		createTime = System.currentTimeMillis();
	}

	public AckerEntry(String sourceSpout, int sourceTaskId, long value) {
		this();
		this.sourceSpout = sourceSpout;
		this.sourceTaskId = sourceTaskId;
		this.value = value;
	}


	/**
	 * @return the sourceSpout
	 */
	public String getSourceSpout() {
		return sourceSpout;
	}

	/**
	 * @return the sourceTaskId
	 */
	public int getSourceTaskId() {
		return sourceTaskId;
	}

	/**
	 * @return the value
	 */
	public long getValue() {
		return value;
	}

	/**
	 * @param value the value to set
	 */
	public void setValue(long value) {
		this.value = value;
	}

	/**
	 * @return the createTime
	 */
	public long getCreateTime() {
		return createTime;
	}

	/**
	 * @return
	 */
	public boolean isComplete() {
		return value == 0;
	}

}
