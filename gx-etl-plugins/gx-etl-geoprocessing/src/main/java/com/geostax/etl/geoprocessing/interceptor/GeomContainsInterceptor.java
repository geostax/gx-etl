package com.geostax.etl.geoprocessing.interceptor;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.flume.Event;
import org.geotools.feature.NameImpl;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.process.Process;
import org.geotools.process.ProcessExecutor;
import org.geotools.process.Processors;
import org.geotools.process.Progress;
import org.geotools.util.KVP;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.Name;

import com.geostax.etl.Context;
import com.geostax.etl.interceptor.Interceptor;
import com.geostax.etl.interceptor.TimestampInterceptor;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;

public class GeomContainsInterceptor implements Interceptor {

	private final String geom;
	Geometry area;
	WKTReader reader;
	ProcessExecutor engine;
	Process process = null;
	FeatureJSON fjson = new FeatureJSON();

	/**
	 * Only {@link TimestampInterceptor.Builder} can build me
	 */
	private GeomContainsInterceptor(String geom) {
		this.geom = geom;
	}

	@Override
	public void initialize() {
		reader = new WKTReader();
		Name name = new NameImpl("geo", "contains");
		process = Processors.createProcess(name);
		engine = Processors.newProcessExecutor(2);
		try {
			area = reader.read(geom);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Modifies events in-place.
	 */
	@Override
	public Event intercept(Event event) {

		InputStream in_nocode = new ByteArrayInputStream(event.getBody());
		try {
			SimpleFeature feature = fjson.readFeature(in_nocode);
			Geometry geometry = (Geometry) feature.getDefaultGeometry();
			Map<String, Object> input = new KVP("a", area);
			input.put("b", geometry);
			Progress working = engine.submit(process, input);
			Map<String, Object> result = working.get(); // get is BLOCKING
			boolean constains = (boolean) result.get("result");
			if (constains) {
				System.out.println(">>>>>> " + new String(event.getBody()));
				return event;
			} else {
				System.out.println(">>>>>> Discard!");
				return null;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return event;
	}

	/**
	 * Delegates to {@link #intercept(Event)} in a loop.
	 * 
	 * @param events
	 * @return
	 */
	@Override
	public List<Event> intercept(List<Event> events) {
		for (Event event : events) {
			intercept(event);
		}
		return events;
	}

	@Override
	public void close() {
		// no-op
	}

	/**
	 * Builder which builds new instances of the TimestampInterceptor.
	 */
	public static class Builder implements Interceptor.Builder {

		private String geom = "";

		@Override
		public Interceptor build() {
			return new GeomContainsInterceptor(geom);
		}

		@Override
		public void configure(Context context) {
			geom = context.getString("geom", "");
		}

	}

	public static class Constants {

	}

}
