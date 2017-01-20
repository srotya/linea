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
package com.srotya.linea;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.uuid.EthernetAddress;
import com.fasterxml.uuid.Generators;
import com.srotya.linea.example.Constants;
import com.srotya.linea.utils.NetUtils;

/**
 * @author ambud
 */
public class Event implements Tuple {

	private static EthernetAddress RNG_ADDRESS;
	public static final int AVG_EVENT_FIELD_COUNT = Integer.parseInt(System.getProperty("event.field.count", "40"));
	private long originEventId;
	private List<Long> sourceIds;
	private long eventId;
	private Map<String, Object> headers;
	private long sourceWorkerId = -1;

	static {
		try {
			RNG_ADDRESS = EthernetAddress.valueOf(NetUtils.selectDefaultIPAddress(false).getHardwareAddress());
		} catch (NumberFormatException | SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Event(String eventId) {
		this();
		// this.eventId = MurmurHash.hash64(eventId);
		// sourceIds = new ArrayList<>();
		// headers = new HashMap<>(AVG_EVENT_FIELD_COUNT);
	}

	public Event() {
		eventId = Generators.timeBasedGenerator(RNG_ADDRESS).generate().getMostSignificantBits();// UUID.randomUUID().getMostSignificantBits();
		sourceIds = new ArrayList<>();
		headers = new HashMap<>(AVG_EVENT_FIELD_COUNT);
	}

	/**
	 * @return
	 */
	public Map<String, Object> getHeaders() {
		return headers;
	}

	/**
	 * @param headers
	 */
	public void setHeaders(Map<String, Object> headers) {
		this.headers = headers;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Event [originEventId=" + originEventId + ", sourceIds=" + sourceIds + ", eventId=" + eventId
				+ ", headers=" + headers + "]";
	}

	@Override
	public Object getGroupByKey() {
		return headers.get(Constants.FIELD_GROUPBY_ROUTING_KEY);
	}

	@Override
	public void setGroupByKey(Object key) {
		headers.put(Constants.FIELD_GROUPBY_ROUTING_KEY, key);
	}

	@Override
	public Object getGroupByValue() {
		return headers.get(Constants.FIELD_GROUP_BY_VALUE);
	}

	@Override
	public void setGroupByValue(Object value) {
		headers.put(Constants.FIELD_GROUP_BY_VALUE, value);
	}

	@Override
	public String getNextBoltId() {
		return (String) headers.get(Constants.FIELD_NEXT_BOLT_ID);
	}

	@Override
	public void setNextBoltId(String nextBoltId) {
		headers.put(Constants.FIELD_NEXT_BOLT_ID, nextBoltId);
	}

	@Override
	public int getDestinationTaskId() {
		return (Integer) headers.get(Constants.FIELD_DESTINATION_TASK_ID);
	}

	@Override
	public void setDestinationTaskId(int taskId) {
		headers.put(Constants.FIELD_DESTINATION_TASK_ID, taskId);
	}

	@Override
	public int getDestinationWorkerId() {
		return (Integer) headers.get(Constants.FIELD_DESTINATION_WORKER_ID);
	}

	@Override
	public void setDestinationWorkerId(int workerId) {
		headers.put(Constants.FIELD_DESTINATION_WORKER_ID, workerId);
	}

	@Override
	public int getTaskId() {
		return (Integer) headers.get(Constants.FIELD_TASK_ID);
	}

	@Override
	public void setTaskId(int taskId) {
		headers.put(Constants.FIELD_TASK_ID, taskId);
	}

	@Override
	public boolean isAck() {
		return (Boolean) headers.get(Constants.FIELD_EVENT_TYPE);
	}

	@Override
	public void setAck(boolean ack) {
		headers.put(Constants.FIELD_EVENT_TYPE, ack);
	}

	@Override
	public String getComponentName() {
		return headers.get(Constants.FIELD_COMPONENT_NAME).toString();
	}

	@Override
	public void setComponentName(String componentName) {
		headers.put(Constants.FIELD_COMPONENT_NAME, componentName);
	}

	@Override
	public void setOriginEventId(long eventId) {
		this.originEventId = eventId;
	}

	@Override
	public void setSourceWorkerId(long workerId) {
		this.sourceWorkerId = workerId;
	}

	@Override
	public long getSourceWorkerId() {
		return sourceWorkerId;
	}

	@Override
	public long getEventId() {
		return eventId;
	}

	@Override
	public List<Long> getSourceIds() {
		return sourceIds;
	}

	@Override
	public long getOriginEventId() {
		return originEventId;
	}

	@Override
	public void setEventId(long eventId) {
		this.eventId = eventId;
	}

}
