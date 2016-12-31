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
package com.srotya.linea;

import java.io.Serializable;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.uuid.EthernetAddress;
import com.fasterxml.uuid.Generators;
import com.srotya.linea.utils.NetUtils;

/**
 * Event implementation.
 * 
 * @author ambud
 */
public class Event implements Serializable {

	private static EthernetAddress RNG_ADDRESS;
	public static final int AVG_EVENT_FIELD_COUNT = Integer.parseInt(System.getProperty("event.field.count", "40"));
	private static final long serialVersionUID = 1L;
	private Long originEventId;
	private List<Long> sourceIds;
	private Long eventId;
	private Map<String, Object> headers;

	static {
		try {
			RNG_ADDRESS = EthernetAddress.valueOf(NetUtils.selectDefaultIPAddress(false).getHardwareAddress());
		} catch (NumberFormatException | SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Event(String eventId) {
		this.eventId = MurmurHash.hash64(eventId);
		sourceIds = new ArrayList<>();
		headers = new HashMap<>(AVG_EVENT_FIELD_COUNT);
	}

	public Event() {
		eventId = Generators.timeBasedGenerator(RNG_ADDRESS).generate().getMostSignificantBits();// UUID.randomUUID().getMostSignificantBits();
		sourceIds = new ArrayList<>();
		headers = new HashMap<>(AVG_EVENT_FIELD_COUNT);
	}

	Event(Map<String, Object> headers) {
		eventId = Generators.timeBasedGenerator(RNG_ADDRESS).generate().getMostSignificantBits();// UUID.randomUUID().getMostSignificantBits();
		sourceIds = new ArrayList<>();
		this.headers = headers;
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

	/**
	 * @return
	 */
	public static Map<String, Object> getMapInstance() {
		return new ConcurrentHashMap<>(AVG_EVENT_FIELD_COUNT);
	}

	/**
	 * @return
	 */
	public Long getEventId() {
		return eventId;
	}

	/**
	 * @param eventId
	 */
	public void setEventId(Long eventId) {
		this.eventId = eventId;
	}

	/**
	 * @return
	 */
	public List<Long> getSourceIds() {
		return sourceIds;
	}

	/**
	 * @param sourceIds
	 */
	public void setSourceIds(List<Long> sourceIds) {
		this.sourceIds = sourceIds;
	}

	/**
	 * @return the originEventId
	 */
	public Long getOriginEventId() {
		return originEventId;
	}

	/**
	 * @param originEventId
	 *            the originEventId to set
	 */
	public void setOriginEventId(Long originEventId) {
		this.originEventId = originEventId;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Event [originEventId=" + originEventId + ", sourceIds=" + sourceIds + ", eventId=" + eventId
				+ ", headers=" + headers + "]";
	}
}
