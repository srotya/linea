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
package com.srotya.linea.clustering;

import static org.junit.Assert.*;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Test;

/**
 * @author ambud
 */
public class TestWorkerEntry {

	@Test
	public void testWorkerEntryComparable() throws UnknownHostException {
		InetAddress addr = InetAddress.getByName("192.168.1.1");
		WorkerEntry entry1 = new WorkerEntry(addr, 1, System.currentTimeMillis());
		addr = InetAddress.getByName("192.168.1.2");
		WorkerEntry entry2 = new WorkerEntry(addr, 1, System.currentTimeMillis());
		assertEquals(-1, entry1.compareTo(entry2));
		System.out.println(entry1.toString());
	}

	@Test
	public void testWorkerEntryEquality() throws UnknownHostException {
		InetAddress addr = InetAddress.getByName("192.168.1.1");
		WorkerEntry entry1 = new WorkerEntry(addr, 1, System.currentTimeMillis());
		addr = InetAddress.getByName("192.168.1.1");
		WorkerEntry entry2 = new WorkerEntry(addr, 1, System.currentTimeMillis());
		assertTrue(entry1.equals(entry2));
	}
}
