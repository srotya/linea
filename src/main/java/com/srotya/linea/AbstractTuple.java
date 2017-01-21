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
import java.util.List;

import com.fasterxml.uuid.EthernetAddress;
import com.fasterxml.uuid.Generators;
import com.srotya.linea.utils.NetUtils;

/**
 * @author ambud
 */
public abstract class AbstractTuple implements Tuple {

	private static EthernetAddress RNG_ADDRESS;
	public static final int AVG_EVENT_FIELD_COUNT = Integer.parseInt(System.getProperty("event.field.count", "40"));
	private long originEventId;
	private List<Long> sourceIds;
	private long eventId;
	private long sourceWorkerId = -1;
	private Object groupByKey;
	private String nextBoltId;
	private int destinationTaskId;
	private int taskId;
	private String componentName;
	private int destinationWorkerId;
	private Object groupByValue;
	private boolean ack;

	static {
		try {
			RNG_ADDRESS = EthernetAddress.valueOf(NetUtils.selectDefaultIPAddress(false).getHardwareAddress());
		} catch (NumberFormatException | SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public AbstractTuple(String eventId) {
		this.eventId = MurmurHash.hash64(eventId);
		sourceIds = new ArrayList<>();
	}

	public AbstractTuple() {
		eventId = Generators.timeBasedGenerator(RNG_ADDRESS).generate().getMostSignificantBits();// UUID.randomUUID().getMostSignificantBits();
		sourceIds = new ArrayList<>();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Event [originEventId=" + originEventId + ", sourceIds=" + sourceIds + ", eventId=" + eventId + "]";
	}

	@Override
	public Object getGroupByKey() {
		return groupByKey;
	}

	@Override
	public void setGroupByKey(Object key) {
		groupByKey = key;
	}

	@Override
	public Object getGroupByValue() {
		return groupByValue;
	}

	@Override
	public void setGroupByValue(Object value) {
		groupByValue = value;
	}

	@Override
	public String getNextBoltId() {
		return nextBoltId;
	}

	@Override
	public void setNextBoltId(String nextBoltId) {
		this.nextBoltId = nextBoltId;
	}

	@Override
	public int getDestinationTaskId() {
		return destinationTaskId;
	}

	@Override
	public void setDestinationTaskId(int taskId) {
		destinationTaskId = taskId;
	}

	@Override
	public int getDestinationWorkerId() {
		return destinationWorkerId;
	}

	@Override
	public void setDestinationWorkerId(int workerId) {
		destinationWorkerId = workerId;
	}

	@Override
	public int getTaskId() {
		return taskId;
	}

	@Override
	public void setTaskId(int taskId) {
		this.taskId = taskId;
	}

	@Override
	public boolean isAck() {
		return ack;
	}

	@Override
	public void setAck(boolean ack) {
		this.ack = ack;
	}

	@Override
	public String getComponentName() {
		return componentName;
	}

	@Override
	public void setComponentName(String componentName) {
		this.componentName = componentName;
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
