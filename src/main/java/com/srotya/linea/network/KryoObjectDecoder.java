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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.srotya.linea.Tuple;

/**
 * Decodes {@link Kryo} Serialized {@link Tuple} objects. This Decoder
 * implementation has refactored utility methods that are used for both TCP and
 * UDP based transports.
 * 
 * @author ambud
 */
public class KryoObjectDecoder {

	public static final ThreadLocal<Kryo> kryoThreadLocal = new ThreadLocal<Kryo>() {
		@Override
		protected Kryo initialValue() {
			Kryo kryo = new Kryo();
			return kryo;
		}
	};

	public KryoObjectDecoder() {
	}

	/**
	 * Deserialize {@link InputStream} to Event using Kryo
	 * 
	 * @param classOf
	 * @param stream
	 * @return
	 * @throws IOException
	 */
	public static <E> E streamToEvent(Class<E> classOf, InputStream stream) throws IOException {
		Input input = new Input(stream);
		try {
			E event = kryoThreadLocal.get().readObject(input, classOf);// InternalTCPTransportServer.kryoThreadLocal.get().readObject(input,
																		// Event.class);
			return event;
		} finally {
		}
	}

}