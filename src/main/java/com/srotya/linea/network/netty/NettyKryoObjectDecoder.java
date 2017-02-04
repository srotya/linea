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
package com.srotya.linea.network.netty;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.srotya.linea.Tuple;
import com.srotya.linea.network.KryoCodec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * Decodes {@link Kryo} Serialized {@link Tuple} objects. This Decoder
 * implementation has refactored utility methods that are used for both TCP and
 * UDP based transports.
 * 
 * @author ambud
 */
public class NettyKryoObjectDecoder<E extends Tuple> extends ByteToMessageDecoder {

	private Class<E> classOf;

	public NettyKryoObjectDecoder(Class<E> classOf) {
		this.classOf = classOf;
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
	public static <E> List<E> bytebufToEvents(Class<E> classOf, ByteBuf in) {
		short count = in.readShort();
		ByteBufInputStream bis = new ByteBufInputStream(in);
		InputStream stream = bis;
		List<E> events = new ArrayList<>(count);
		Input input = new Input(stream);
		int i = 0;
		try {
			for (; i < count; i++) {
				E event = KryoCodec.kryoThreadLocal.get().readObject(input, classOf);
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
	
	public static <E> E bytebufToEvent(Class<E> classOf, ByteBuf in) throws IOException {
		Input input = new Input(new ByteBufInputStream(in));
		return KryoCodec.streamToEvent(classOf, input);
	}

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
		System.out.println("Event");
		if (in == null) {
			return;
		}
//		List<E> tuples = bytebufToEvents(classOf, in);
//		out.addAll(tuples);
		E tuple = bytebufToEvent(classOf, in);
		out.add(tuple);
	}

}