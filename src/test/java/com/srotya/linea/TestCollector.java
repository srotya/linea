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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.srotya.linea.network.Router;
import com.srotya.linea.processors.BoltExecutor;
import com.srotya.linea.tolerance.AckerBolt;

/**
 * Unit tests for {@link Collector}
 * 
 * @author ambud
 */
@RunWith(MockitoJUnitRunner.class)
public class TestCollector {

	@Mock
	private Router<TestTuple> router;

	@Test
	public void testConstructor() {
		TestTupleFactory factory = new TestTupleFactory();
		Collector<TestTuple> collector = new Collector<>(factory, router, "testBolt", 1, 2);
		assertEquals(factory, collector.getFactory());
		assertEquals(router, collector.getRouter());
		assertEquals(1, collector.getlTaskId());
		assertEquals(2, collector.getParallelism());
	}

	@Test
	public void testEventAck() {
		TestTupleFactory factory = new TestTupleFactory();
		@SuppressWarnings("unchecked")
		Router<TestTuple> router = mock(Router.class);
		final AtomicReference<TestTuple> ackEvent = new AtomicReference<TestTuple>(null);
		Mockito.doAnswer(new Answer<Void>() {

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				ackEvent.set(invocation.getArgumentAt(0, TestTuple.class));
				return null;
			}
		}).when(router).routeTuple(any(TestTuple.class));
		Collector<TestTuple> collector = new Collector<>(factory, router, "testBolt", 1, 2);
		TestTuple tuple = factory.buildTuple();
		tuple.getSourceIds().add(11023231L);
		collector.ack(tuple);
		// 2 ack tuples expected
		verify(router, times(1)).routeTuple(any(TestTuple.class));
		TestTuple ackTuple = ackEvent.get();
		assertEquals(AckerBolt.ACKER_BOLT_NAME, ackTuple.getNextBoltId());
		assertEquals(0, ackTuple.getDestinationTaskId());
		assertEquals(11023231L, ackTuple.getGroupByKey());
		assertEquals(tuple.getTupleId(), ackTuple.getGroupByValue());
	}

	@Test
	public void testSpoutEmit() {
		TestTupleFactory factory = new TestTupleFactory();
		@SuppressWarnings("unchecked")
		Router<TestTuple> router = mock(Router.class);
		final AtomicReference<TestTuple> outputTuple = new AtomicReference<TestTuple>(null);
		Mockito.doAnswer(new Answer<Void>() {

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				outputTuple.set(invocation.getArgumentAt(0, TestTuple.class));
				return null;
			}
		}).when(router).routeTuple(any(TestTuple.class));
		Collector<TestTuple> collector = new Collector<>(factory, router, "testSpout", 1, 2);
		TestTuple tuple = factory.buildTuple();
		collector.spoutEmit("testBolt", tuple);
		verify(router, times(2)).routeTuple(any(TestTuple.class));
		TestTuple nextTuple = outputTuple.get();
		assertEquals(tuple, nextTuple);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testEmitDirect() {
		TestTupleFactory factory = new TestTupleFactory();
		Router<TestTuple> router = mock(Router.class);
		final AtomicReference<TestTuple> outputTuple = new AtomicReference<TestTuple>(null);
		Mockito.doAnswer(new Answer<Void>() {

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				outputTuple.set(invocation.getArgumentAt(0, TestTuple.class));
				return null;
			}
		}).when(router).routeToTaskId(any(TestTuple.class), any(BoltExecutor.class), any(Integer.class));
		Collector<TestTuple> collector = new Collector<>(factory, router, "testBolt", 1, 2);
		TestTuple tuple = factory.buildTuple();
		collector.emitDirect("testBolt2", 2, tuple);
		assertEquals(tuple, outputTuple.get());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testEmitWithAnchor() {
		TestTupleFactory factory = new TestTupleFactory();
		Router<TestTuple> router = mock(Router.class);
		final AtomicReference<TestTuple> outputTuple = new AtomicReference<TestTuple>(null);
		Mockito.doAnswer(new Answer<Void>() {

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				outputTuple.set(invocation.getArgumentAt(0, TestTuple.class));
				return null;
			}
		}).when(router).routeTuple(any(TestTuple.class));
		Collector<TestTuple> collector = new Collector<>(factory, router, "testBolt", 1, 2);
		TestTuple tuple = factory.buildTuple();
		TestTuple anchorTuple = factory.buildTuple();
		anchorTuple.setOriginTupleId(anchorTuple.getTupleId());
		collector.emit("testBolt2", tuple, anchorTuple);
		verify(router, times(2)).routeTuple(any(TestTuple.class));
		assertEquals(tuple, outputTuple.get());
	}
}
