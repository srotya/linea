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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.srotya.linea.Tuple;

/**
 * Decodes {@link Kryo} Serialized {@link Tuple} objects. This Decoder
 * implementation has refactored utility methods that are used for both TCP and
 * UDP based transports.
 * 
 * @author ambud
 */
public class KryoCodec {

	public static final ThreadLocal<Kryo> kryoThreadLocal = new ThreadLocal<Kryo>() {
		@Override
		protected Kryo initialValue() {
			Kryo kryo = new Kryo();
			return kryo;
		}
	};

	public KryoCodec() {
	}

	/**
	 * Kryo serialize {@link Tuple} to byte array
	 * 
	 * @param event
	 *            serialized as byte array
	 * @return
	 * @throws IOException
	 */
	public static <E> byte[] eventToByteArray(E event) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
		OutputStream os = bos;
		Output output = new Output(os);
		kryoThreadLocal.get().writeObject(output, event);
		output.close();
		return bos.toByteArray();
	}

	/**
	 * Deserialize {@link InputStream} to Event using Kryo
	 * 
	 * @param classOf
	 * @param stream
	 * @return
	 * @throws IOException
	 */
	public static <E> E streamToEvent(Class<E> classOf, Input input) throws IOException {
		E event = kryoThreadLocal.get().readObject(input, classOf);
		return event;
	}

}