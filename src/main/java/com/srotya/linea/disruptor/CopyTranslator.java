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
package com.srotya.linea.disruptor;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.srotya.linea.Tuple;

/**
 * Translator used by Disruptor to copy events into a {@link RingBuffer}
 * 
 * @author ambud
 */
public abstract class CopyTranslator<E extends Tuple> implements EventTranslatorOneArg<E, E> {
	
	@Override
	public void translateTo(E outputTuple, long sequence, E inputTuple) {
		outputTuple.getSourceIds().clear();
		outputTuple.getSourceIds().addAll(inputTuple.getSourceIds());
		outputTuple.setEventId(inputTuple.getTupleId());
		outputTuple.setSourceWorkerId(inputTuple.getSourceWorkerId());
		outputTuple.setOriginTupleId(inputTuple.getOriginTupleId());
		outputTuple.setGroupByKey(inputTuple.getGroupByKey());
		outputTuple.setGroupByValue(inputTuple.getGroupByValue());
		outputTuple.setNextBoltId(inputTuple.getNextBoltId());
		outputTuple.setDestinationTaskId(inputTuple.getDestinationTaskId());
		outputTuple.setTaskId(inputTuple.getTaskId());
		outputTuple.setDestinationWorkerId(inputTuple.getDestinationWorkerId());
		outputTuple.setComponentName(inputTuple.getComponentName());
		outputTuple.setAck(inputTuple.isAck());
		translate(outputTuple, sequence, inputTuple);
	}

	protected abstract void translate(E outputEvent, long sequence, E inputEvent);

}
