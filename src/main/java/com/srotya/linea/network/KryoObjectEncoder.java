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
import java.io.OutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.srotya.linea.Tuple;
import com.srotya.linea.network.nio.TCPServer;

/**
 * {@link Kryo} serializes {@link Tuple} for Netty transmission
 * 
 * @author ambud
 */
public class KryoObjectEncoder {
	
	/**
	 * Kryo serialize {@link Tuple} to {@link OutputStream}
	 * @param event
	 * @param stream
	 * @throws IOException
	 */
	public static <E> void writeEventToStream(E event, OutputStream stream) throws IOException {
		Output output = new Output(stream);
		TCPServer.kryoThreadLocal.get().writeObject(output, event);
		output.flush();
	}

	/**
	 * Kryo serialize {@link Tuple} to byte array
	 * @param event serialized as byte array
	 * @return
	 * @throws IOException
	 */
	public static <E> byte[] eventToByteArray(E event) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
		OutputStream os = bos;
		Output output = new Output(os);
		TCPServer.kryoThreadLocal.get().writeObject(output, event);
		output.close();
		return bos.toByteArray();
	}

}