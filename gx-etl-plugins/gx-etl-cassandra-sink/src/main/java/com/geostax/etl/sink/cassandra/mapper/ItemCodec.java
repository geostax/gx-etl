package com.geostax.etl.sink.cassandra.mapper;

import java.nio.ByteBuffer;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class ItemCodec extends TypeCodec<Item> {

	private final TypeCodec<UDTValue> innerCodec;

	private final UserType userType;

	public ItemCodec(TypeCodec<UDTValue> innerCodec, Class<Item> javaType) {
		super(innerCodec.getCqlType(), javaType);
		this.innerCodec = innerCodec;
		this.userType = (UserType) innerCodec.getCqlType();
	}

	@Override
	public ByteBuffer serialize(Item value, ProtocolVersion protocolVersion) throws InvalidTypeException {
		return innerCodec.serialize(toUDTValue(value), protocolVersion);
	}

	@Override
	public Item deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
		return toItem(innerCodec.deserialize(bytes, protocolVersion));
	}

	@Override
	public Item parse(String value) throws InvalidTypeException {
		return value == null || value.isEmpty() || value.equals("") ? null : toItem(innerCodec.parse(value));
	}

	@Override
	public String format(Item value) throws InvalidTypeException {
		return value == null ? null : innerCodec.format(toUDTValue(value));
	}

	protected Item toItem(UDTValue value) {
		return value == null ? null
				: new Item(value.getString("name"), value.getString("description"), value.getString("type"),
						value.getString("unit"), value.getDouble("maxvalue"), value.getDouble("minvalue"));
	}

	protected UDTValue toUDTValue(Item value) {
		return value == null ? null
				: userType.newValue().setString("name", value.getName())
						.setString("description", value.getDescription()).setString("type", value.getType())
						.setString("unit", value.getUnit()).setDouble("maxvalue", value.getMaxvalue())
						.setDouble("minvalue", value.getMinvalue());
	}
}