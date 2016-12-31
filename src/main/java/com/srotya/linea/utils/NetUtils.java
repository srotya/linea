/**
 * Copyright 2016 Ambud Sharma
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * 
 * You may obtain a copy of the License at
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.srotya.linea.utils;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

/**
 * Network utilities
 * 
 * @author ambud
 */
public class NetUtils {

	private NetUtils() {
	}

	/**
	 * Find broadcast address for multicast gossip implementations
	 * 
	 * @return get broadcast address
	 * @throws SocketException
	 */
	public static InetAddress getBroadcastAddress() throws SocketException {
		Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
		while (interfaces.hasMoreElements()) {
			NetworkInterface networkInterface = interfaces.nextElement();
			if (networkInterface.isLoopback() || !networkInterface.supportsMulticast()) {
				continue;
			}
			for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
				InetAddress broadcast = interfaceAddress.getBroadcast();
				if (broadcast == null) {
					continue;
				} else {
					return broadcast;
				}
			}
		}
		return null;
	}

	/**
	 * Convert long (primitive) to 8 bytes
	 * 
	 * @param l
	 * @return bytes
	 */
	public static byte[] longToBytes(long l) {
		byte[] result = new byte[8];
		for (int i = 7; i >= 0; i--) {
			result[i] = (byte) (l & 0xFF);
			l >>= 8;
		}
		return result;
	}

	/**
	 * Convert 8 bytes to long (primitive)
	 * 
	 * @param b
	 * @return long
	 */
	public static long bytesToLong(byte[] b) {
		long result = 0;
		for (int i = 0; i < 8; i++) {
			result <<= 8;
			result |= (b[i] & 0xFF);
		}
		return result;
	}

	/**
	 * Convert 32 bit integer IP to string representation 
	 * @param ip
	 * @return ip string
	 */
	public static String toStringIP(int ip) {
		StringBuilder builder = new StringBuilder();
		builder.append(((ip >> 24) & 0xff) + ".");
		builder.append(((ip >> 16) & 0xff) + ".");
		builder.append(((ip >> 8) & 0xff) + ".");
		builder.append(((ip >> 0) & 0xff));
		return builder.toString();
	}

	/**
	 * Convert string IP to 32 bit integer
	 * @param ip
	 * @return ip integer
	 */
	public static int stringIPtoInt(String ip) {
		String[] ipParts = ip.split("\\.");
		int intIP = 0;
		for (int i = 0; i < 4; i++) {
			intIP += Integer.parseInt(ipParts[i]) << (24 - (8 * i));
		}
		return intIP;
	}
	
	/**
	 * Auto select network interface, to be used for binding sockets
	 * @param loopback
	 * @return networkInterface
	 * @throws SocketException
	 */
	public static NetworkInterface selectDefaultIPAddress(boolean loopback) throws SocketException {
		Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
		while (ifaces.hasMoreElements()) {
			NetworkInterface iface = ifaces.nextElement();
			if (loopback && iface.isLoopback()) {
				return iface;
			} else if (iface.isPointToPoint() || iface.isVirtual()) {
				continue;
			}
			if (!iface.isLoopback() && iface.isUp()) {
				return iface;
			}
		}
		return null;
	}

	/**
	 * Get IP address of a network interface 
	 * @param iface
	 * @return ipAddress
	 */
	public static Inet4Address getIPv4Address(NetworkInterface iface) {
		for (InterfaceAddress interfaceAddress : iface.getInterfaceAddresses()) {
			if (interfaceAddress.getAddress() instanceof Inet4Address) {
				return (Inet4Address) interfaceAddress.getAddress();
			}
		}
		return null;
	}

}
