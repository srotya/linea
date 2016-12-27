package com.srotya.linea;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;

import org.junit.Test;

import com.srotya.linea.Event;
import com.srotya.linea.network.KryoObjectDecoder;
import com.srotya.linea.network.KryoObjectEncoder;

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
		Event e2 = KryoObjectDecoder.streamToEvent(stream);

		assertEquals(e1.getEventId(), e2.getEventId());
	}

}
