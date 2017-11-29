package com.geostax.etl.sink.cassandra.mapper;

public class Item {

	String name;
	String description;
	String type;
	String unit;
	double maxvalue=0;
	double minvalue=0;
	
	public Item() {
		// TODO Auto-generated constructor stub
	}
	
	public Item(String name, String description, String type, String unit, double maxvalue, double minvalue) {
		super();
		this.name = name;
		this.description = description;
		this.type = type;
		this.unit = unit;
		this.maxvalue = maxvalue;
		this.minvalue = minvalue;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getUnit() {
		return unit;
	}

	public void setUnit(String unit) {
		this.unit = unit;
	}

	public double getMaxvalue() {
		return maxvalue;
	}

	public void setMaxvalue(double maxvalue) {
		this.maxvalue = maxvalue;
	}

	public double getMinvalue() {
		return minvalue;
	}

	public void setMinvalue(double minvalue) {
		this.minvalue = minvalue;
	}
	
}
