package com.geostax.etl.sink.cassandra.util;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;

/**
 * This class contains a set of Geometry utility methods that are generally
 * useful throughout the GeoWave core codebase
 */
public class GeometryUtils
{
	public static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
	private static final int DEFAULT_DIMENSIONALITY = 2;


	/**
	 * Converts a JTS geometry to binary using JTS a Well Known Binary writer
	 *
	 * @param geometry
	 *            The JTS geometry
	 * @return The binary representation of the geometry
	 */
	public static byte[] geometryToBinary(
			final Geometry geometry ) {

		int dimensions = DEFAULT_DIMENSIONALITY;

		if (!geometry.isEmpty()) {
			dimensions = Double.isNaN(geometry.getCoordinate().getOrdinate(
					Coordinate.Z)) ? 2 : 3;
		}
		return new WKBWriter(
				dimensions).write(geometry);
	}

	/**
	 * Converts a byte array as well-known binary to a JTS geometry
	 *
	 * @param binary
	 *            The well known binary
	 * @return The JTS geometry
	 */
	public static Geometry geometryFromBinary(
			final byte[] binary ) {
		try {
			return new WKBReader().read(binary);
		}
		catch (final ParseException e) {
			
		}
		return null;
	}

	/**
	 * This mehtod returns an envelope between negative infinite and positive
	 * inifinity in both x and y
	 *
	 * @return the infinite bounding box
	 */
	public static Geometry infinity() {
		// unless we make this synchronized, we will want to instantiate a new
		// geometry factory because geometry factories are not thread safe
		return new GeometryFactory().toGeometry(new Envelope(
				Double.NEGATIVE_INFINITY,
				Double.POSITIVE_INFINITY,
				Double.NEGATIVE_INFINITY,
				Double.POSITIVE_INFINITY));
	}
}
