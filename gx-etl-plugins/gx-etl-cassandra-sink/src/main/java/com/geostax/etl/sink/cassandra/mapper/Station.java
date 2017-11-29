package com.geostax.etl.sink.cassandra.mapper;

import java.nio.ByteBuffer;
import java.util.UUID;

public class Station {

	private UUID id;
	private String fea_pos;
	private String stationid;
	private UUID catalogid;
	private String name;
	private String region;
	private String metadata;
	private double x;
	private double y;
	private double z;
	private ByteBuffer geometry;

	public Station() {
		// TODO Auto-generated constructor stub
	}

	public Station(UUID id, String fea_pos, String stationid, UUID catalogid, String name, String region,
			String metadata, double x, double y, double z, ByteBuffer geometry) {
		super();
		this.id = id;
		this.fea_pos = fea_pos;
		this.stationid = stationid;
		this.catalogid = catalogid;
		this.name = name;
		this.region = region;
		this.metadata = metadata;
		this.x = x;
		this.y = y;
		this.z = z;
		this.geometry = geometry;
	}

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public String getFea_pos() {
		return fea_pos;
	}

	public void setFea_pos(String fea_pos) {
		this.fea_pos = fea_pos;
	}

	public String getStationid() {
		return stationid;
	}

	public void setStationid(String stationid) {
		this.stationid = stationid;
	}

	public UUID getCatalogid() {
		return catalogid;
	}

	public void setCatalogid(UUID catalogid) {
		this.catalogid = catalogid;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}

	public String getMetadata() {
		return metadata;
	}

	public void setMetadata(String metadata) {
		this.metadata = metadata;
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

	public ByteBuffer getGeometry() {
		return geometry;
	}

	public void setGeometry(ByteBuffer geometry) {
		this.geometry = geometry;
	}

}
