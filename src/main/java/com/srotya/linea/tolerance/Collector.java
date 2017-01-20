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

import com.srotya.linea.Tuple;
import com.srotya.linea.TupleFactory;
import com.srotya.linea.network.Router;

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
public class Collector<E extends Tuple> {

	public static final String FIELD_ACK_EVENT = "_ack";
	private int lTaskId;
	private String lComponentId;
	private TupleFactory<E> factory;
	private Router<E> router;
	private int workerId;

	/**
	 * @param factory
	 * @param router
	 * @param lComponentId
	 * @param taskId
	 */
	public Collector(TupleFactory<E> factory, Router<E> router, String lComponentId, int taskId) {
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
	public void ack(E event) {
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
	protected void ack(String spoutName, long sourceEventId, long currentEventId, int taskId) {
		E event = factory.buildEvent();
		event.setOriginEventId(sourceEventId);
		event.setAck(true); // for debug purposes
		event.setGroupByKey(sourceEventId);
		event.setGroupByValue(currentEventId);
		event.setComponentName(spoutName);
		event.setTaskId(taskId);
		router.routeEvent(AckerBolt.ACKER_BOLT_NAME, event);
	}

	/**
	 * Method to be used only by Spout when it generates a new {@link Tuple}.
	 * This method will trigger creation of a new Event processing tree. (same
	 * as a tuple tree Storm)
	 * 
	 * @param nextProcessorId
	 * @param event
	 */
	public void spoutEmit(String nextProcessorId, E event) {
		event.setComponentName(lComponentId);
		event.setTaskId(lTaskId);
		event.setOriginEventId(event.getEventId());
		event.setSourceWorkerId(workerId);
		emit(nextProcessorId, event, event);
	}

	/**
	 * Emit {@link Tuple} directly to a task. This method circumvents the
	 * {@link Router}'s destination calculation logic. (To be used with extreme
	 * care)
	 * 
	 * @param nextProcessor
	 * @param destinationTaskId
	 * @param event
	 */
	public void emitDirect(String nextProcessor, int destinationTaskId, E event) {
		event.setTaskId(lTaskId);
		event.setComponentName(lComponentId);
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
	public void emit(String nextProcessorId, Map<String, Object> outputEventHeaders, E anchorEvent) {
		E outputEvent = factory.buildEvent();
		outputEvent.setOriginEventId(anchorEvent.getOriginEventId());
		outputEvent.getSourceIds().add(anchorEvent.getOriginEventId());
		outputEvent.setTaskId(lTaskId);
		outputEvent.setComponentName(lComponentId);
		ack(anchorEvent.getComponentName(), anchorEvent.getOriginEventId(), outputEvent.getEventId(),
				anchorEvent.getTaskId());
		router.routeEvent(nextProcessorId, outputEvent);
	}

	/**
	 * Internally used for Spout {@link Tuple} emission.
	 * 
	 * @param nextProcessorId
	 * @param outputEvent
	 * @param anchorEvent
	 */
	protected void emit(String nextProcessorId, E outputEvent, E anchorEvent) {
		outputEvent.setOriginEventId(anchorEvent.getOriginEventId());
		outputEvent.getSourceIds().add(anchorEvent.getOriginEventId());
		outputEvent.setTaskId(lTaskId);
		outputEvent.setComponentName(lComponentId);
		ack(anchorEvent.getComponentName(), anchorEvent.getOriginEventId(), outputEvent.getEventId(),
				anchorEvent.getTaskId());
		router.routeEvent(nextProcessorId, outputEvent);
	}

	/**
	 * @return factory
	 */
	public TupleFactory<E> getFactory() {
		return factory;
	}

}
