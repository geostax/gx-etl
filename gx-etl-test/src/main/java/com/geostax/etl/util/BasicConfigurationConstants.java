package com.geostax.etl.util;

public final class BasicConfigurationConstants {

	public static final String CONFIG_SOURCES = "sources";
	public static final String CONFIG_SOURCES_PREFIX = CONFIG_SOURCES + ".";
	public static final String CONFIG_SOURCE_CHANNELSELECTOR_PREFIX = "selector.";

	public static final String CONFIG_SINKS = "sinks";
	public static final String CONFIG_SINKS_PREFIX = CONFIG_SINKS + ".";
	public static final String CONFIG_SINK_PROCESSOR_PREFIX = "processor.";

	public static final String CONFIG_SINKGROUPS = "sinkgroups";
	public static final String CONFIG_SINKGROUPS_PREFIX = CONFIG_SINKGROUPS + ".";

	public static final String CONFIG_CHANNEL = "channel";
	public static final String CONFIG_CHANNELS = "channels";
	public static final String CONFIG_CHANNELS_PREFIX = CONFIG_CHANNELS + ".";

	public static final String CONFIG_CONFIG = "config";
	public static final String CONFIG_TYPE = "type";

	private BasicConfigurationConstants() {
		// disable explicit object creation
	}

}