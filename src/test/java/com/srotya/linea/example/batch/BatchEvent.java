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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.srotya.linea.AbstractTuple;

/**
 * @author ambud
 */
public class BatchEvent extends AbstractTuple {

	public static final int AVG_EVENT_FIELD_COUNT = Integer.parseInt(System.getProperty("event.field.count", "40"));
	private List<Map<String, Object>> batch;

	public BatchEvent(String eventId) {
		super(eventId);
		batch = new ArrayList<>();
	}

	public BatchEvent() {
		batch = new ArrayList<>();
	}

	/**
	 * @return the batch
	 */
	public List<Map<String, Object>> getBatch() {
		return batch;
	}

	/**
	 * @param batch the batch to set
	 */
	public void setBatch(List<Map<String, Object>> batch) {
		this.batch = batch;
	}

}
