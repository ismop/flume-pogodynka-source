package pl.cyfronet.ismop.flume;

import java.util.Date;

public class Reading {
	
	private Date timestamp;
	
	private Float value;
	
	public Reading(Date timestamp, Float value) {
		super();
		this.timestamp = timestamp;
		this.value = value;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public Float getValue() {
		return value;
	}
	
}
