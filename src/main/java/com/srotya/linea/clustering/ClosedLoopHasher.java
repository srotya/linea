package com.srotya.linea.clustering;

import com.srotya.linea.utils.NetUtils;

public class ClosedLoopHasher {

	public static void main(String[] args) {
		int count = 20;
		for (int i = 0; i < count; i++) {
			int ipAddress = NetUtils.stringIPtoInt("172.16.21.1"+i);
			System.out.println(ipAddress % 10);
		}
	}

}
