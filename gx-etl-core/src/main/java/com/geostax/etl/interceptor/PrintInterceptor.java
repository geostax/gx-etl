package com.geostax.etl.interceptor;

import static com.geostax.etl.interceptor.TimestampInterceptor.Constants.CONFIG_HEADER_NAME;
import static com.geostax.etl.interceptor.TimestampInterceptor.Constants.CONFIG_PRESERVE;
import static com.geostax.etl.interceptor.TimestampInterceptor.Constants.DEFAULT_HEADER_NAME;
import static com.geostax.etl.interceptor.TimestampInterceptor.Constants.DEFAULT_PRESERVE;

import java.util.List;
import java.util.Map;

import org.apache.flume.Event;

import com.geostax.etl.Context;

public class PrintInterceptor implements Interceptor {

	private final boolean preserveExisting;

	/**
	 * Only {@link TimestampInterceptor.Builder} can build me
	 */
	private PrintInterceptor(boolean preserveExisting) {
		this.preserveExisting = preserveExisting;
	}

	@Override
	public void initialize() {
		// no-op
	}

	/**
	 * Modifies events in-place.
	 */
	@Override
	public Event intercept(Event event) {
		System.out.println(">>>>>> "+new String(event.getBody()));
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

		private boolean preserveExisting = DEFAULT_PRESERVE;

		@Override
		public Interceptor build() {
			return new PrintInterceptor(preserveExisting);
		}

		@Override
		public void configure(Context context) {
			preserveExisting = context.getBoolean(CONFIG_PRESERVE, DEFAULT_PRESERVE);
		}

	}

	public static class Constants {
		public static final String CONFIG_PRESERVE = "preserveExisting";
		public static final boolean DEFAULT_PRESERVE = false;
		public static final String CONFIG_HEADER_NAME = "headerName";
		public static final String DEFAULT_HEADER_NAME = "timestamp";
	}

}
