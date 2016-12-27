/**
 * Copyright 2016 Ambud Sharma
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
package com.srotya.linea;

import java.util.HashMap;
import java.util.Map;

/**
 * Unified factory implementation for Tau
 * 
 * @author ambudsharma
 */
public class UnifiedFactory implements EventFactory {
	
	@Override
	public Event buildEvent() {
		return new Event();
	}
	
	@Override
	public Event buildEvent(String eventId) {
		return new Event(eventId);
	}
	
	/**
	 * @param headers
	 * @return
	 */
	public Event buildEvent(Map<String, Object> headers) {
		return new Event(headers);
	}
	
	public static Map<String, String> getSubmap(String contains, Map<String, String> stormConf) {
		Map<String, String> conf = new HashMap<>();
		for(Object key:stormConf.keySet()) {
			if(key.toString().contains(contains)) {
				conf.put(key.toString(), stormConf.get(key).toString());
			}
		}
		return conf;
	}

}