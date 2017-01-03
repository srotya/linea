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

import java.util.Map;

import com.srotya.linea.Event;
import com.srotya.linea.EventFactory;
import com.srotya.linea.network.Router;
import com.srotya.linea.utils.Constants;

/**
 * A collector is responsible for gather events and acks that need to
 * transmitted out of a bolt/spout. Collector is essentially a bridge between a
 * Bolt and {@link Router} and performs some necessary enrichment of information
 * for correct routing.<br>
 * <br>
 * Collector also provides Event factory that can be used to create new events.
 * <br>
 * <br>
 * There is a collector object per Bolt Instance.
 * 
 * @author ambud
 */
public class Collector {

	public static final String FIELD_ACK_EVENT = "_ack";
	private int lTaskId;
	private String lComponentId;
	private EventFactory factory;
	private Router router;
	private int workerId;

	/**
	 * @param factory
	 * @param router
	 * @param lComponentId
	 * @param taskId
	 */
	public Collector(EventFactory factory, Router router, String lComponentId, int taskId) {
		this.factory = factory;
		this.router = router;
		this.lComponentId = lComponentId;
		this.lTaskId = taskId;
		this.workerId = router.getSelfWorkerId();
	}

	/**
	 * Ack event
	 * 
	 * @param event
	 */
	public void ack(Event event) {
		for (Long sourceEventId : event.getSourceIds()) {
			ack(lComponentId, sourceEventId, event.getEventId(), lTaskId);
		}
	}

	/**
	 * Collector internal ack method.
	 * 
	 * @param spoutName
	 * @param sourceEventId
	 * @param currentEventId
	 * @param taskId
	 */
	protected void ack(String spoutName, Long sourceEventId, Long currentEventId, Integer taskId) {
		Event event = factory.buildEvent();
		event.setOriginEventId(sourceEventId);
		event.getHeaders().put(FIELD_ACK_EVENT, true); // for debug purposes
		event.getHeaders().put(Constants.FIELD_GROUPBY_ROUTING_KEY, sourceEventId);
		event.getHeaders().put(Constants.FIELD_AGGREGATION_VALUE, currentEventId);
		event.getHeaders().put(Constants.FIELD_COMPONENT_NAME, spoutName);
		event.getHeaders().put(Constants.FIELD_TASK_ID, taskId);
		router.routeEvent(AckerBolt.ACKER_BOLT_NAME, event);
	}

	/**
	 * Method to be used only by Spout when it generates a new {@link Event}.
	 * This method will trigger creation of a new Event processing tree. (same
	 * as a tuple tree Storm)
	 * 
	 * @param nextProcessorId
	 * @param event
	 */
	public void spoutEmit(String nextProcessorId, Event event) {
		event.getHeaders().put(Constants.FIELD_COMPONENT_NAME, lComponentId);
		event.getHeaders().put(Constants.FIELD_TASK_ID, lTaskId);
		event.setOriginEventId(event.getEventId());
		event.setSourceWorkerId(workerId);
		emit(nextProcessorId, event, event);
	}

	/**
	 * Emit {@link Event} directly to a task. This method circumvents the
	 * {@link Router}'s destination calculation logic. (To be used with extreme
	 * care)
	 * 
	 * @param nextProcessor
	 * @param destinationTaskId
	 * @param event
	 */
	public void emitDirect(String nextProcessor, Integer destinationTaskId, Event event) {
		event.getHeaders().put(Constants.FIELD_TASK_ID, lTaskId);
		event.getHeaders().put(Constants.FIELD_COMPONENT_NAME, lComponentId);
		router.routeToTaskId(nextProcessor, event, null, destinationTaskId);
	}

	/**
	 * Emit event to next processor id. Event object is generated internally
	 * from the supplied headers.
	 * 
	 * @param nextProcessorId
	 * @param outputEventHeaders
	 * @param anchorEvent
	 */
	public void emit(String nextProcessorId, Map<String, Object> outputEventHeaders, Event anchorEvent) {
		Event outputEvent = factory.buildEvent();
		outputEvent.getHeaders().putAll(outputEventHeaders);
		outputEvent.setOriginEventId(anchorEvent.getOriginEventId());
		outputEvent.getSourceIds().add(anchorEvent.getOriginEventId());
		outputEvent.getHeaders().put(Constants.FIELD_TASK_ID, lTaskId);
		outputEvent.getHeaders().put(Constants.FIELD_COMPONENT_NAME, lComponentId);
		ack((String) anchorEvent.getHeaders().get(Constants.FIELD_COMPONENT_NAME), anchorEvent.getOriginEventId(),
				outputEvent.getEventId(), (Integer) anchorEvent.getHeaders().get(Constants.FIELD_TASK_ID));
		router.routeEvent(nextProcessorId, outputEvent);
	}

	/**
	 * Internally used for Spout {@link Event} emission.
	 * 
	 * @param nextProcessorId
	 * @param outputEvent
	 * @param anchorEvent
	 */
	protected void emit(String nextProcessorId, Event outputEvent, Event anchorEvent) {
		outputEvent.setOriginEventId(anchorEvent.getOriginEventId());
		outputEvent.getSourceIds().add(anchorEvent.getOriginEventId());
		outputEvent.getHeaders().put(Constants.FIELD_TASK_ID, lTaskId);
		outputEvent.getHeaders().put(Constants.FIELD_COMPONENT_NAME, lComponentId);
		ack((String) anchorEvent.getHeaders().get(Constants.FIELD_COMPONENT_NAME), anchorEvent.getOriginEventId(),
				outputEvent.getEventId(), (Integer) anchorEvent.getHeaders().get(Constants.FIELD_TASK_ID));
		router.routeEvent(nextProcessorId, outputEvent);
	}

	// public void emit(String nextProcessorId, Event outputEvent, Event...
	// anchorEvents) {
	// outputEvent.getHeaders().put(Constants.FIELD_TASK_ID, taskId);
	// for (Event anchorEvent : anchorEvents) {
	// outputEvent.getSourceIds().add(anchorEvent.getEventId());
	// outputEvent.setOriginEventId(anchorEvent.getOriginEventId());
	// outputEvent.getHeaders().put(Constants.FIELD_SPOUT_NAME,
	// (String) anchorEvent.getHeaders().get(Constants.FIELD_SPOUT_NAME));
	// ack((String) anchorEvent.getHeaders().get(Constants.FIELD_SPOUT_NAME),
	// anchorEvent.getOriginEventId(),
	// anchorEvent.getEventId(), outputEvent.getEventId());
	// }
	// router.routeEvent(nextProcessorId, outputEvent);
	// }

	/**
	 * @return factory
	 */
	public EventFactory getFactory() {
		return factory;
	}

}
