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
package com.srotya.linea.tolerance;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import com.srotya.linea.Event;
import com.srotya.linea.disruptor.ROUTING_TYPE;
import com.srotya.linea.processors.Bolt;
import com.srotya.linea.utils.Constants;

/**
 * Inspired by the XOR Ledger concept of Apache Storm by Nathan Marz. <br>
 * <br>
 * Acker an efficient tracking mechanism for tracking "tuple trees" regardless
 * of the amount of branching there may be in a deterministic memory space using
 * a fast XOR operator.
 * 
 * @author ambud
 */
public class AckerBolt implements Bolt {

	private static final Logger logger = Logger.getLogger(AckerBolt.class.getName());
	private static final int PRINT_COUNT = 100000;
	private static final long serialVersionUID = 1L;
	public static final String ACKER_BOLT_NAME = "_acker";
	private static final float ACKER_MAP_LOAD_FACTOR = 0.9f;
	private static final int ACKER_MAP_SIZE = 1000000;
	private transient RotatingMap<Long, AckerEntry> ackerMap;
	private transient int taskId;
	private transient Collector collector;
	private transient int c;

	public AckerBolt() {
	}

	@Override
	public void configure(Map<String, String> conf, int taskId, Collector collector) {
		this.taskId = taskId;
		this.collector = collector;
		ackerMap = new RotatingMap<>(3);
	}

	@Override
	public ROUTING_TYPE getRoutingType() {
		return ROUTING_TYPE.GROUPBY;
	}

	@Override
	public String getBoltName() {
		return ACKER_BOLT_NAME;
	}

	@Override
	public void process(Event event) {
		boolean isBroadcast = false;
		if (isBroadcast) {
			// tick event
			expireEvents();
		} else {
			Object sourceId = event.getHeaders().get(Constants.FIELD_GROUPBY_ROUTING_KEY);
			String source = (String) event.getHeaders().get(Constants.FIELD_COMPONENT_NAME);
			updateAckerMap(source, event.getHeaders().get(Constants.FIELD_TASK_ID), (Long) sourceId,
					(Long) event.getHeaders().get(Constants.FIELD_AGGREGATION_VALUE));
		}
	}

	/**
	 * Expire events in the Map that have not yet received all the acks.
	 */
	public void expireEvents() {
		Map<Long, AckerEntry> evictionEntries = ackerMap.rotate();
		for (Entry<Long, AckerEntry> entry : evictionEntries.entrySet()) {
			if (entry.getValue().isComplete()) {
				// exception; entry was asynchronously acked
			} else {
				// notify source
				Event event = collector.getFactory().buildEvent();
				event.setOriginEventId(entry.getKey());
				event.getHeaders().put(Constants.FIELD_GROUPBY_ROUTING_KEY, entry.getKey());
				event.getHeaders().put(Constants.FIELD_EVENT_TYPE, true);
				event.getHeaders().put("sourceSpout", entry.getValue().getSourceSpout());
				collector.emitDirect(entry.getValue().getSourceSpout(), entry.getValue().getSourceTaskId(), event);
			}
		}
	}

	/**
	 * Update Rotating map for this ack. This may lead to an updated XOR and if
	 * the XOR value is 0 then results in an Ack Event to the source Spout.
	 * 
	 * @param source
	 * @param sourceTaskId
	 * @param sourceId
	 * @param nextEvent
	 */
	public void updateAckerMap(String source, Object sourceTaskId, Long sourceId, Long nextEvent) {
		AckerEntry trackerValue = ackerMap.get(sourceId);
		if (trackerValue == null) {
			if (!source.contains("Spout")) {
				// reject message
				logger.severe("Incorrect event ordering:" + sourceId + "\t" + source + "\t" + "\t" + taskId);
				return;
			}
			// this is the first time we are seeing this event
			trackerValue = new AckerEntry(source, (Integer) sourceTaskId, sourceId);
			ackerMap.put(sourceId, trackerValue);
		} else {
			// event tree xor logic
			trackerValue.setValue(trackerValue.getValue() ^ nextEvent);
			if (trackerValue.isComplete()) {
				// means event processing tree is complete
				c++;
				if (c % PRINT_COUNT == 0) {
					logger.info("Acked " + PRINT_COUNT + ":" + taskId);
				}
				logger.fine("Acking event:" + sourceId + "\t" + trackerValue.getSourceSpout());

				// remove entry from ackerMap
				ackerMap.remove(sourceId);

				// notify source that event's completely processed
				Event event = collector.getFactory().buildEvent();
				event.setOriginEventId(sourceId);
				event.getHeaders().put(Constants.FIELD_GROUPBY_ROUTING_KEY, sourceId);
				event.getHeaders().put(Constants.FIELD_EVENT_TYPE, true);
				event.getHeaders().put("sourceSpout", trackerValue.getSourceSpout());
				collector.emitDirect(trackerValue.getSourceSpout(), trackerValue.getSourceTaskId(), event);
			}
		}
	}

	/**
	 * Rotating map is used to efficiently track different acker entries for a
	 * timed eviction of expired events.
	 * 
	 * @author ambud
	 *
	 * @param <K>
	 * @param <V>
	 */
	public static class RotatingMap<K, V> {

		private LinkedList<Map<K, V>> buckets;

		public RotatingMap(int bucketCount) {
			buckets = new LinkedList<>();
			for (int i = 0; i < bucketCount; i++) {
				buckets.add(new HashMap<>(ACKER_MAP_SIZE / bucketCount, ACKER_MAP_LOAD_FACTOR));
			}
		}

		public Map<K, V> rotate() {
			Map<K, V> expiredMap = buckets.remove();
			buckets.addLast(new HashMap<>());
			return expiredMap;
		}

		public void put(K key, V value) {
			buckets.getLast().put(key, value);
		}

		public V get(K key) {
			for (Map<K, V> map : buckets) {
				V value = map.get(key);
				if (value != null) {
					return value;
				}
			}
			return null;
		}

		public V remove(K key) {
			for (Map<K, V> map : buckets) {
				V remove = map.remove(key);
				if (remove != null) {
					return remove;
				}
			}
			return null;
		}

		@Override
		public String toString() {
			return buckets.toString();
		}

	}

	@Override
	public void ready() {
	}

}
