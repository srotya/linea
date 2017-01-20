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

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;

import org.junit.Test;

import com.srotya.linea.Tuple;
import com.srotya.linea.network.KryoObjectDecoder;
import com.srotya.linea.network.KryoObjectEncoder;

/**
 * @author ambud
 */
public class TestKryoSerialization {

	@Test
	public void testSerializationSize() throws Exception {
		Event event = new Event();
		event.getHeaders().put("host", "xyz.srotya.com");
		event.getHeaders().put("message",
				"ix-dc9-19.ix.netcom.com - - [04/Sep/1995:00:00:28 -0400] \"GET /html/cgi.html HTTP/1.0\" 200 2217\r\n");
		event.getHeaders().put("value", 10);
		event.setEventId(13123134234L);
		byte[] ary = KryoObjectEncoder.eventToByteArray(event);
		System.out.println("Without Compression Array Length:" + ary.length);
		ary = KryoObjectEncoder.eventToByteArray(event);
		System.out.println("With Compression Array Length:" + ary.length);
	}

	@Test
	public void testEncoderDecoder() throws Exception {
		Event e1 = new Event();
		e1.getHeaders().put("host", "xyz.srotya.com");
		e1.getHeaders().put("message",
				"ix-dc9-19.ix.netcom.com - - [04/Sep/1995:00:00:28 -0400] \"GET /html/cgi.html HTTP/1.0\" 200 2217\r\n");
		e1.getHeaders().put("value", 10);
		e1.setEventId(13123134234L);
		byte[] ary = KryoObjectEncoder.eventToByteArray(e1);

		ByteArrayInputStream stream = new ByteArrayInputStream(ary);
		Tuple e2 = KryoObjectDecoder.streamToEvent(Event.class, stream);

		assertEquals(e1.getEventId(), e2.getEventId());
	}

}
