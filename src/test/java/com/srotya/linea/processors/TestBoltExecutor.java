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
package com.srotya.linea.processors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.srotya.linea.Collector;
import com.srotya.linea.TestTuple;
import com.srotya.linea.TestTupleFactory;
import com.srotya.linea.clustering.Columbus;
import com.srotya.linea.disruptor.ROUTING_TYPE;
import com.srotya.linea.disruptor.TestCopyTranslator;
import com.srotya.linea.network.Router;
import com.srotya.linea.processors.BoltExecutor.BoltExecutorWrapper;

/**
 * Unit tests for {@link BoltExecutor}
 * 
 * @author ambud
 */
@RunWith(MockitoJUnitRunner.class)
public class TestBoltExecutor {

	@Mock
	private Columbus columbus;
	@Mock
	private Router<TestTuple> router;

	@Test
	public void testConstructorBolt() throws IOException, ClassNotFoundException {
		TestBoltInstance testBoltInstance = new TestBoltInstance();
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		ObjectOutputStream ois = new ObjectOutputStream(stream);
		ois.writeObject(testBoltInstance);
		ois.close();
		byte[] bolt = stream.toByteArray();
		BoltExecutor<TestTuple> executor = new BoltExecutor<TestTuple>(new HashMap<>(), new TestTupleFactory(), bolt,
				columbus, 2, router, new TestCopyTranslator.TestTupleTranslator());
		assertEquals(2, executor.getParallelism());
		assertEquals(testBoltInstance.getBoltName(), executor.getTemplateBoltInstance().getBoltName());
		assertEquals(testBoltInstance.getRoutingType(), executor.getTemplateBoltInstance().getRoutingType());
	}

	@Test
	public void testConstructorSpout() throws IOException, ClassNotFoundException {
		TestSpoutInstance testBoltInstance = new TestSpoutInstance();
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		ObjectOutputStream ois = new ObjectOutputStream(stream);
		ois.writeObject(testBoltInstance);
		ois.close();
		byte[] bolt = stream.toByteArray();
		BoltExecutor<TestTuple> executor = new BoltExecutor<TestTuple>(new HashMap<>(), new TestTupleFactory(), bolt,
				columbus, 2, router, new TestCopyTranslator.TestTupleTranslator());
		assertEquals(2, executor.getParallelism());
		assertEquals(testBoltInstance.getBoltName(), executor.getTemplateBoltInstance().getBoltName());
		assertEquals(testBoltInstance.getRoutingType(), executor.getTemplateBoltInstance().getRoutingType());
	}

	@Test
	public void testSpoutStart() throws IOException, ClassNotFoundException, InterruptedException {
		TestSpoutInstance testBoltInstance = new TestSpoutInstance();
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		ObjectOutputStream ois = new ObjectOutputStream(stream);
		ois.writeObject(testBoltInstance);
		ois.close();
		byte[] bolt = stream.toByteArray();
		BoltExecutor<TestTuple> executor = new BoltExecutor<TestTuple>(new HashMap<>(), new TestTupleFactory(), bolt,
				columbus, 2, router, new TestCopyTranslator.TestTupleTranslator());
		executor.start();
		Map<Integer, BoltExecutorWrapper<TestTuple>> map = executor.getTaskProcessorMap();
		assertEquals(2, map.size());
		assertNotNull(map.entrySet().iterator().next().getValue().getBuffer());
		assertTrue(!executor.getEs().isTerminated());
		executor.stop();
		assertTrue(executor.getEs().isShutdown());
	}

	@Test
	public void testBoltStart() throws IOException, ClassNotFoundException, InterruptedException {
		TestBoltInstance testBoltInstance = new TestBoltInstance();
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		ObjectOutputStream ois = new ObjectOutputStream(stream);
		ois.writeObject(testBoltInstance);
		ois.close();
		byte[] bolt = stream.toByteArray();
		BoltExecutor<TestTuple> executor = new BoltExecutor<TestTuple>(new HashMap<>(), new TestTupleFactory(), bolt,
				columbus, 2, router, new TestCopyTranslator.TestTupleTranslator());
		executor.start();
		Map<Integer, BoltExecutorWrapper<TestTuple>> map = executor.getTaskProcessorMap();
		assertEquals(2, map.size());
		assertNotNull(map.entrySet().iterator().next().getValue().getBuffer());
		assertTrue(!executor.getEs().isTerminated());
		executor.stop();
		assertTrue(executor.getEs().isShutdown());
	}

	@Test
	public void testProcessTuple() throws IOException, ClassNotFoundException, InterruptedException {
		TestBoltInstance testBoltInstance = new TestBoltInstance();
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		ObjectOutputStream ois = new ObjectOutputStream(stream);
		ois.writeObject(testBoltInstance);
		ois.close();
		byte[] bolt = stream.toByteArray();
		TestTupleFactory factory = new TestTupleFactory();
		BoltExecutor<TestTuple> executor = new BoltExecutor<TestTuple>(new HashMap<>(), factory, bolt, columbus, 2,
				router, new TestCopyTranslator.TestTupleTranslator());
		executor.start();
		TestTuple tuple = factory.buildTuple();
		tuple.getSourceIds().add(tuple.getTupleId());
		tuple.setField("ping");
		executor.process(0, tuple);
		Thread.sleep(10);
		
		// 1 ack should be received
		verify(router, times(1)).routeTuple(any(TestTuple.class));
		executor.process(4, tuple);
		executor.stop();
		verify(router, times(1)).routeTuple(any(TestTuple.class));
	}

	public static class TestSpoutInstance extends Spout<TestTuple> {

		private static final long serialVersionUID = 1L;

		@Override
		public void configure(Map<String, String> conf, int instanceId, Collector<TestTuple> collector) {
		}

		@Override
		public void ready() {
		}

		@Override
		public String getBoltName() {
			return "testSpout";
		}

		@Override
		public void ack(Long eventId) {
		}

		@Override
		public void fail(Long eventId) {
		}

	}

	public static class TestBoltInstance implements Bolt<TestTuple> {

		private static final long serialVersionUID = 1L;
		private transient Collector<TestTuple> collector;

		@Override
		public void configure(Map<String, String> conf, int instanceId, Collector<TestTuple> collector) {
			this.collector = collector;
		}

		@Override
		public void ready() {
		}

		@Override
		public void process(TestTuple tuple) {
			collector.ack(tuple);
		}

		@Override
		public ROUTING_TYPE getRoutingType() {
			return ROUTING_TYPE.SHUFFLE;
		}

		@Override
		public String getBoltName() {
			return "testBolt";
		}

	}

}
