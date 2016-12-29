package com.srotya.linea.network;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.srotya.linea.Event;
import com.srotya.linea.network.netty.InternalTCPTransportServer;

import io.netty.handler.codec.bytes.ByteArrayEncoder;

/**
 * {@link Kryo} serializes {@link Event} for Netty transmission
 * 
 * @author ambud
 */
public class KryoObjectEncoder extends ByteArrayEncoder {
	
	public static void writeEventToStream(Event event, OutputStream stream) throws IOException {
		Output output = new Output(stream);
		InternalTCPTransportServer.kryoThreadLocal.get().writeObject(output, event);
		output.flush();
	}

	public static byte[] eventToByteArray(Event event) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
		OutputStream os = bos;
		Output output = new Output(os);
		InternalTCPTransportServer.kryoThreadLocal.get().writeObject(output, event);
		output.close();
		return bos.toByteArray();
	}

}