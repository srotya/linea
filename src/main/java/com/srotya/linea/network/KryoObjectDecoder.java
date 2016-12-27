package com.srotya.linea.network;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.srotya.linea.Event;
import com.srotya.linea.network.netty.InternalTCPTransportServer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * Decodes {@link Kryo} Serialized {@link Event} objects. This Decoder
 * implementation has refactored utility methods that are used for both TCP and
 * UDP based transports.
 * 
 * @author ambud
 */
public class KryoObjectDecoder extends ByteToMessageDecoder {

	public KryoObjectDecoder() {
	}

	/**
	 * Takes a Netty {@link ByteBuf} as input and returns a list of events
	 * deserialized from the buffer. <br>
	 * The buffer must be length prefixed to get the number of events in the
	 * buffer.
	 * 
	 * @param in
	 * @return list of tauEvents
	 */
	public static List<Event> bytebufToEvents(ByteBuf in) {
		short count = in.readShort();
		ByteBufInputStream bis = new ByteBufInputStream(in);
		InputStream stream = bis;
		List<Event> events = new ArrayList<>(count);
		Input input = new Input(stream);
		int i = 0;
		try {
			for (; i < count; i++) {
				Event event = InternalTCPTransportServer.kryoThreadLocal.get().readObject(input, Event.class);
				events.add(event);
			}
			return events;
		} catch (Exception e) {
			System.err.println("FAILURE to read count of events:" + count + "\tat i=" + i);
			e.printStackTrace();
			throw e;
		} finally {
			input.close();
		}
	}

	/**
	 * Deserializes a single {@link Event} from a Netty {@link ByteBuf}
	 * 
	 * @param in
	 * @return tauEvent
	 * @throws IOException
	 */
	public static Event byteBufToEvent(ByteBuf in) throws IOException {
		ByteBufInputStream bis = new ByteBufInputStream(in);
		InputStream stream = bis;
		return streamToEvent(stream);
	}

	/**
	 * @param stream
	 * @return
	 * @throws IOException
	 */
	public static Event streamToEvent(InputStream stream) throws IOException {
		Input input = new Input(stream);
		try {
			Kryo kryo = new Kryo();
			Event event = kryo.readObject(input, Event.class);// InternalTCPTransportServer.kryoThreadLocal.get().readObject(input, Event.class);
			return event;
		} finally {
		}
	}
	
	public static Event streamToEvent(Input input) throws IOException {
		try {
			Event event = InternalTCPTransportServer.kryoThreadLocal.get().readObject(input, Event.class);
			return event;
		} finally {
		}
	}

	/**
	 * Deserializes {@link List} of {@link Event}s from a byte array
	 * 
	 * @param bytes
	 * @param skip
	 *            prefix bytes to skip
	 * @param count
	 *            of events to read
	 * @return list of tauEvents
	 * @throws IOException
	 */
	public static List<Event> bytesToEvent(byte[] bytes, int skip, int count) throws IOException {
		List<Event> events = new ArrayList<>();
		Input input = new Input(bytes);
		input.skip(skip);
		try {
			for (int i = 0; i < count; i++) {
				Event event = InternalTCPTransportServer.kryoThreadLocal.get().readObject(input, Event.class);
				events.add(event);
			}
			return events;
		} finally {
			input.close();
		}
	}


	@Override
	protected void decode(ChannelHandlerContext arg0, ByteBuf frame, List<Object> output) throws Exception {
		if (frame == null) {
		}
		Event event = byteBufToEvent(frame);
		output.add(event);
	}

}