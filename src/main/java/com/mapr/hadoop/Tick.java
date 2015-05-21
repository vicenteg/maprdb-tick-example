package com.mapr.hadoop;

import java.util.HashMap;
import org.joda.time.DateTime;

public class Tick {
	public Integer m;
	public Integer s;
	public Integer mi;
	public HashMap<String,Double> tick;
	
	public Tick() {
		tick = new HashMap<String, Double>();
	}
	
	public void setTime(DateTime dt) {
		m = dt.getMinuteOfHour();
		s = dt.getSecondOfMinute();
		mi = dt.getMillisOfSecond();
	}
	
	public void setTick(String type, Double value) {
		tick.put(type,  value);
	}
}
