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
package com.srotya.linea;

/**
 * @author ambud
 */
public interface Tuple {
	
	public long getEventId();
	
	public Object getGroupByKey();
	
	public void setGroupByKey(Object key);
	
	public Object getGroupByValue();
	
	public void setGroupByValue(Object value);
	
	public String getNextBoltId();
	
	public void setNextBoltId(String nextBoltId);
	
	public int getDestinationTaskId();
	
	public void setDestinationTaskId(int taskId);
	
	public int getDestinationWorkerId();
	
	public void setDestinationWorkerId(int taskId);
	
	public int getTaskId();
	
	public void setTaskId(int taskId);
	
	public boolean isAck();
	
	public void setAck(boolean ack);
	
	public String getComponentName();
	
	public void setComponentName(String componentName);
	
	public long[] getSourceIds();
	
	public void addSourceId(long id);
	
	public void setOriginEventId(long eventId);
	
	public long getOriginEventId();
	
	public void setOriginWorkerId(long workerId);
	
	public long getOriginWorkerId();
	
}
