/**
 * Copyright 2016 Ambud Sharma
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * 
 * You may obtain a copy of the License at
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.srotya.linea;

import static org.junit.Assert.*;

import org.junit.Test;

import com.srotya.linea.utils.NetUtils;

/**
 * @author ambud
 */
public class TestUtils {

	@Test
	public void testLongByteConversion() {
		byte[] bytes = NetUtils.longToBytes(453245);
		long value = NetUtils.bytesToLong(bytes);
		assertEquals(453245, value);
	}

	@Test
	public void testIPConversion() {
		int ip = NetUtils.stringIPtoInt("192.168.1.2");
		String ipString = NetUtils.toStringIP(ip);
		assertEquals("192.168.1.2", ipString);
	}
}
