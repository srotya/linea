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

import static org.junit.Assert.*;

import org.junit.Test;

import com.srotya.linea.TestTuple;
import com.srotya.linea.TestTupleFactory;

/**
 * Unit tests for {@link CopyTranslator}
 * 
 * @author ambud
 */
public class TestCopyTranslator {

	@Test
	public void testTranslateTo() {
		TestTupleFactory factory = new TestTupleFactory();
		TestTuple input = factory.buildTuple();
		input.setAck(true);
		input.setComponentName("test");
		input.setDestinationTaskId(2);
		input.setDestinationWorkerId(2);
		input.setGroupByKey("test2");
		input.setGroupByValue("val");
		input.setOriginTupleId(123213L);
		input.setTaskId(1);
		input.setNextBoltId("uid");
		input.getSourceIds().add(13123123L);
		input.setSourceWorkerId(22);
		TestTuple output = factory.buildTuple();
		CopyTranslator<TestTuple> copy = new TestTupleTranslator();
		copy.translateTo(output, 1L, input);
		assertEquals(input.getComponentName(), output.getComponentName());
		assertEquals(input.getDestinationTaskId(), output.getDestinationTaskId());
		assertEquals(input.getDestinationWorkerId(), output.getDestinationWorkerId());
		assertEquals(input.getGroupByKey(), output.getGroupByKey());
		assertEquals(input.getGroupByValue(), output.getGroupByValue());
		assertEquals(input.getNextBoltId(), output.getNextBoltId());
		assertEquals(input.getOriginTupleId(), output.getOriginTupleId());
		assertEquals(input.getSourceIds(), output.getSourceIds());
		assertEquals(input.getSourceWorkerId(), output.getSourceWorkerId());
		assertEquals(input.getTupleId(), output.getTupleId());
		assertEquals(input.getTaskId(), output.getTaskId());
	}

	/**
	 * @author ambud
	 */
	public static class TestTupleTranslator extends CopyTranslator<TestTuple> {

		@Override
		protected void translate(TestTuple outputEvent, long sequence, TestTuple inputEvent) {
			outputEvent.setField(inputEvent.getField());
		}

	}

}
