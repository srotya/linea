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
import com.srotya.linea.Event;
import com.srotya.linea.network.nio.TCPServer;

/**
 * {@link Kryo} serializes {@link Event} for Netty transmission
 * 
 * @author ambud
 */
public class KryoObjectEncoder {
	
	/**
	 * Kryo serialize {@link Event} to {@link OutputStream}
	 * @param event
	 * @param stream
	 * @throws IOException
	 */
	public static void writeEventToStream(Event event, OutputStream stream) throws IOException {
		Output output = new Output(stream);
		TCPServer.kryoThreadLocal.get().writeObject(output, event);
		output.flush();
	}

	/**
	 * Kryo serialize {@link Event} to byte array
	 * @param event
	 * @return event serialized as byte array
	 * @throws IOException
	 */
	public static byte[] eventToByteArray(Event event) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
		OutputStream os = bos;
		Output output = new Output(os);
		TCPServer.kryoThreadLocal.get().writeObject(output, event);
		output.close();
		return bos.toByteArray();
	}

}