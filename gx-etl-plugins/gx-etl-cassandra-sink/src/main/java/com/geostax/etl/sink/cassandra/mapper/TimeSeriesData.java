package com.geostax.etl.sink.cassandra.mapper;

import java.util.Map;
import java.util.UUID;

public class TimeSeriesData {

	private String station_id;
	private String station_name;
	private UUID catalog_id;
	private double x;
	private double y;
	private double z;
	private Map<Long,Map<String,Object>> data;
	
	public String getStation_id() {
		return station_id;
	}
	public void setStation_id(String station_id) {
		this.station_id = station_id;
	}
	public String getStation_name() {
		return station_name;
	}
	public void setStation_name(String station_name) {
		this.station_name = station_name;
	}
	public UUID getCatalog_id() {
		return catalog_id;
	}
	public void setCatalog_id(UUID catalog_id) {
		this.catalog_id = catalog_id;
	}
	public double getX() {
		return x;
	}
	public void setX(double x) {
		this.x = x;
	}
	public double getY() {
		return y;
	}
	public void setY(double y) {
		this.y = y;
	}
	public double getZ() {
		return z;
	}
	public void setZ(double z) {
		this.z = z;
	}
	public Map<Long, Map<String, Object>> getData() {
		return data;
	}
	public void setData(Map<Long, Map<String, Object>> data) {
		this.data = data;
	}
	
	
	
}
