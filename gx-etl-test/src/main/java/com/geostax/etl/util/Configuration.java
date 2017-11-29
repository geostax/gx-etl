package com.geostax.etl.util;

import java.util.List;
import java.util.Map;

public class Configuration {

	private Map<String, Source> sources;
	private Map<String, Sink> sinks;
	private Map<String, Channel> chennels;
	private Map<String, Interceptor> interceptors;

	public String toFlumeConfig() {

		return "";
	}

	public String toMxGraphConfig() {
		return "";
	}

	public static Configuration fromFlumeConfig(String filename) {
		Configuration conf = new Configuration();
		return conf;
	}

	public static Configuration fromMxGraphConfig(String filename) {
		Configuration conf = new Configuration();
		return conf;
	}

	class Source {

		private String name;
		private String type;
		private List<String> channels;
		private List<String> interceptors;
		private Map<String, String> params;

	}

	class Sink {
		private String name;
		private String type;
		private List<String> channels;
		private Map<String, String> params;
	}

	class Channel {
		private String name;
		private String type;
		private Map<String, String> params;
	}

	class Interceptor {
		private String name;
		private String type;
		private Map<String, String> params;
	}
}
