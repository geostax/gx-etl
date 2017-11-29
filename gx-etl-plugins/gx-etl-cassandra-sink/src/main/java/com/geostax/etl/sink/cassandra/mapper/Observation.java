package com.geostax.etl.sink.cassandra.mapper;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class Observation {

	private Date timestamp;
	private String stationid;
	private Map<String,Object> records;
	
	public Date getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}
	public String getStationid() {
		return stationid;
	}
	public void setStationid(String stationid) {
		this.stationid = stationid;
	}
	public Map<String, Object> getRecords() {
		return records;
	}
	public void setRecords(Map<String, Object> records) {
		this.records = records;
	}
	
	
}


