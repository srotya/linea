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

import com.srotya.linea.network.Router;
import com.srotya.linea.tolerance.AckerBolt;

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
	private int parallelism;

	/**
	 * @param factory
	 * @param router
	 * @param lComponentId
	 * @param taskId
	 */
	public Collector(TupleFactory<E> factory, Router<E> router, String lComponentId, int taskId, int parallelism) {
		this.factory = factory;
		this.router = router;
		this.lComponentId = lComponentId;
		this.lTaskId = taskId;
		this.parallelism = parallelism;
		this.workerId = router.getSelfWorkerId();
	}

	/**
	 * Ack event
	 * 
	 * @param tuple
	 */
	public void ack(E tuple) {
		for (Long sourceTupleId : tuple.getSourceIds()) {
			ack(lComponentId, sourceTupleId, tuple.getTupleId(), lTaskId);
		}
	}

	/**
	 * Collector internal ack method.
	 * 
	 * @param spoutName
	 * @param sourceTupleId
	 * @param currentTupleId
	 * @param taskId
	 */
	protected void ack(String spoutName, long sourceTupleId, long currentTupleId, int taskId) {
		E ackTuple = factory.buildTuple();
		ackTuple.setOriginTupleId(sourceTupleId);
		ackTuple.setAck(true); // for debug purposes
		ackTuple.setGroupByKey(sourceTupleId);
		ackTuple.setGroupByValue(currentTupleId);
		ackTuple.setComponentName(spoutName);
		ackTuple.setTaskId(taskId);
		ackTuple.setNextBoltId(AckerBolt.ACKER_BOLT_NAME);
		router.routeTuple(ackTuple);
	}

	/**
	 * Method to be used only by Spout when it generates a new {@link Tuple}.
	 * This method will trigger creation of a new Event processing tree. (same
	 * as a tuple tree Storm)
	 * 
	 * @param nextProcessorId
	 * @param tuple
	 */
	public void spoutEmit(String nextProcessorId, E tuple) {
		tuple.setComponentName(lComponentId);
		tuple.setTaskId(lTaskId);
		tuple.setOriginTupleId(tuple.getTupleId());
		tuple.setSourceWorkerId(workerId);
		emit(nextProcessorId, tuple, tuple);
	}

	/**
	 * Emit {@link Tuple} directly to a task. This method circumvents the
	 * {@link Router}'s destination calculation logic. (To be used with extreme
	 * care)
	 * 
	 * @param nextBolt
	 * @param destinationTaskId
	 * @param tuple
	 */
	public void emitDirect(String nextBolt, int destinationTaskId, E tuple) {
		tuple.setTaskId(lTaskId);
		tuple.setComponentName(lComponentId);
		tuple.setNextBoltId(nextBolt);
		router.routeToTaskId(tuple, null, destinationTaskId);
	}

	/**
	 * Internally used for Spout {@link Tuple} emission.
	 * 
	 * @param nextProcessorId
	 * @param outputTuple
	 * @param anchorTuple
	 */
	public void emit(String nextProcessorId, E outputTuple, E anchorTuple) {
		outputTuple.setOriginTupleId(anchorTuple.getOriginTupleId());
		outputTuple.getSourceIds().add(anchorTuple.getOriginTupleId());
		outputTuple.setTaskId(lTaskId);
		outputTuple.setComponentName(lComponentId);
		ack(anchorTuple.getComponentName(), anchorTuple.getOriginTupleId(), outputTuple.getTupleId(),
				anchorTuple.getTaskId());
		outputTuple.setNextBoltId(nextProcessorId);
		router.routeTuple(outputTuple);
	}

	/**
	 * @return factory
	 */
	public TupleFactory<E> getFactory() {
		return factory;
	}

	/**
	 * @return the lTaskId
	 */
	protected int getlTaskId() {
		return lTaskId;
	}

	/**
	 * @return the router
	 */
	protected Router<E> getRouter() {
		return router;
	}

	/**
	 * @return the parallelism
	 */
	protected int getParallelism() {
		return parallelism;
	}

}
