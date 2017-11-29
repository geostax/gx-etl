package com.geostax.etl.sink.cassandra.mapper;

import java.util.Map;
import java.util.UUID;

public class Record {

	private String feature_id;
	private String record_id;
	private long timestamp;
	private Map<String, Object> data;

	public String getFeature_id() {
		return feature_id;
	}

	public void setFeature_id(String feature_id) {
		this.feature_id = feature_id;
	}

	public String getRecord_id() {
		return record_id;
	}

	public void setRecord_id(String record_id) {
		this.record_id = record_id;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public Map<String, Object> getData() {
		return data;
	}

	public void setData(Map<String, Object> data) {
		this.data = data;
	}

}
