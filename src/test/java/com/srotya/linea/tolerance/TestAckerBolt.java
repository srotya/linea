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

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.srotya.linea.Collector;
import com.srotya.linea.TestTuple;
import com.srotya.linea.TestTupleFactory;
import com.srotya.linea.disruptor.ROUTING_TYPE;
import com.srotya.linea.network.Router;
import com.srotya.linea.processors.BoltExecutor;

/**
 * Unit tests for {@link AckerBolt}
 * 
 * @author ambud
 */
public class TestAckerBolt {

	@SuppressWarnings("unchecked")
	@Test
	public void testConfigure() {
		AckerBolt<TestTuple> bolt = new AckerBolt<>();
		TestTupleFactory factory = new TestTupleFactory();
		Router<TestTuple> router = mock(Router.class);
		final AtomicReference<TestTuple> ackEvent = new AtomicReference<TestTuple>(null);
		Mockito.doAnswer(new Answer<Void>() {

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				ackEvent.set(invocation.getArgumentAt(0, TestTuple.class));
				return null;
			}
		}).when(router).routeToTaskId(any(TestTuple.class), any(BoltExecutor.class), any(Integer.class));
		Collector<TestTuple> collector = new Collector<>(factory, router, AckerBolt.ACKER_BOLT_NAME, 0, 1);
		bolt.configure(new HashMap<>(), 0, collector);
		assertEquals(3, bolt.getAckerMap().size());
		assertEquals(ROUTING_TYPE.GROUPBY, bolt.getRoutingType());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testAckTuple() {
		AckerBolt<TestTuple> bolt = new AckerBolt<>();
		TestTupleFactory factory = new TestTupleFactory();
		Router<TestTuple> router = mock(Router.class);
		final AtomicReference<TestTuple> ackEvent = new AtomicReference<TestTuple>(null);
		Mockito.doAnswer(new Answer<Void>() {

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				ackEvent.set(invocation.getArgumentAt(0, TestTuple.class));
				return null;
			}
		}).when(router).routeToTaskId(any(TestTuple.class), any(BoltExecutor.class), any(Integer.class));
		Collector<TestTuple> collector = new Collector<>(factory, router, AckerBolt.ACKER_BOLT_NAME, 0, 1);
		bolt.configure(new HashMap<>(), 0, collector);

		TestTuple ackTuple = collector.getFactory().buildTuple();
		ackTuple.setComponentName("testSpout");
		ackTuple.setGroupByKey(11231231L);
		ackTuple.setGroupByValue(11231231L);

		bolt.process(ackTuple);

		assertEquals(11231231L, bolt.getAckerMap().get(11231231L).getValue());
		// ack it second time to complete tuple tree
		bolt.process(ackTuple);
		// acker should evict this entry since it's been fully acked now
		assertEquals(null, bolt.getAckerMap().get(11231231L));
		verify(router, times(1)).routeToTaskId(ackEvent.get(), null, 0);
		assertEquals(0, ackEvent.get().getDestinationTaskId());
		assertEquals("testSpout", ackEvent.get().getNextBoltId());

		ackTuple = collector.getFactory().buildTuple();
		ackTuple.setComponentName("test");
		ackTuple.setGroupByKey(11231231L);
		ackTuple.setGroupByValue(11231231L);

		bolt.process(ackTuple);
		assertEquals(null, bolt.getAckerMap().get(11231231L));
	}

}
