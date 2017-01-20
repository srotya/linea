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
package com.srotya.linea.network;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.srotya.linea.network.nio.TCPServer;

/**
 * Decodes {@link Kryo} Serialized {@link Tuple} objects. This Decoder
 * implementation has refactored utility methods that are used for both TCP and
 * UDP based transports.
 * 
 * @author ambud
 */
public class KryoObjectDecoder {

	public KryoObjectDecoder() {
	}

	/**
	 * Deserialize {@link InputStream} to Event using Kryo
	 * @param stream
	 * @return
	 * @throws IOException
	 */
	public static <E> E streamToEvent(Class<E> classOf, InputStream stream) throws IOException {
		Input input = new Input(stream);
		try {
			Kryo kryo = new Kryo();
			E event = kryo.readObject(input, classOf);// InternalTCPTransportServer.kryoThreadLocal.get().readObject(input, Event.class);
			return event;
		} finally {
		}
	}
	
	/**
	 * @param input
	 * @return
	 * @throws IOException
	 */
	public static <E> E streamToEvent(Class<E> classOf, Input input) throws IOException {
		try {
			E event = TCPServer.kryoThreadLocal.get().readObject(input, classOf);
			return event;
		} finally {
		}
	}

	/**
	 * Deserializes {@link List} of {@link Tuple}s from a byte array
	 * 
	 * @param bytes
	 * @param skip
	 *            prefix bytes to skip
	 * @param count
	 *            of events to read
	 * @return list of tauEvents
	 * @throws IOException
	 */
	public static <E> List<E> bytesToEvent(Class<E> classOf, byte[] bytes, int skip, int count) throws IOException {
		List<E> events = new ArrayList<>();
		Input input = new Input(bytes);
		input.skip(skip);
		try {
			for (int i = 0; i < count; i++) {
				E event = TCPServer.kryoThreadLocal.get().readObject(input, classOf);
				events.add(event);
			}
			return events;
		} finally {
			input.close();
		}
	}

}