package com.geostax.etl.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.geostax.etl.Context;
import com.geostax.etl.configuration.BasicConfigurationConstants;
import com.geostax.etl.configuration.FlumeConfiguration;
import com.geostax.etl.configuration.FlumeConfigurationError;
import com.geostax.etl.configuration.FlumeConfigurationErrorType;
import com.geostax.etl.configuration.FlumeConfiguration.ComponentNameAndConfigKey;
import com.geostax.etl.configuration.FlumeConfigurationError.ErrorOrWarning;
import com.geostax.etl.node.PropertiesFileConfigurationProvider;
import com.google.common.collect.Maps;

public class ConfigTransformer {

	private static final String DEFAULT_PROPERTIES_IMPLEMENTATION = "java.util.Properties";

	private final File file;

	public ConfigTransformer(File file) {
		this.file = file;
	}

	public void tranform() {
		BufferedReader reader = null;

		try {
			reader = new BufferedReader(new FileReader(file));
			String resolverClassName = System.getProperty("propertiesImplementation",
					DEFAULT_PROPERTIES_IMPLEMENTATION); // =>
														// java.util.Properties
			Class<? extends Properties> propsclass = Class.forName(resolverClassName).asSubclass(Properties.class);
			Properties properties = propsclass.newInstance();
			properties.load(reader);

			Map<String, String> result = Maps.newHashMap();
			Enumeration<?> propertyNames = properties.propertyNames();
			String agentName = null;
			String sources = null;
			String sinks = null;
			String channels = null;
			while (propertyNames.hasMoreElements()) {
				String name = (String) propertyNames.nextElement();
				String value = properties.getProperty(name);
				result.put(name, value);

				int index = name.indexOf('.');
				agentName = name.substring(0, index);
				String configKey = name.substring(index + 1);

				// Check for sources
				if (configKey.equals(BasicConfigurationConstants.CONFIG_SOURCES)) {
					if (sources == null) {
						sources = value;
					}
				}

				// Check for sinks
				if (configKey.equals(BasicConfigurationConstants.CONFIG_SINKS)) {
					if (sinks == null) {
						sinks = value;
					}
				}

				// Check for channels
				if (configKey.equals(BasicConfigurationConstants.CONFIG_CHANNELS)) {
					if (channels == null) {
						channels = value;
					}
				}

				ComponentNameAndConfigKey cnck = parseConfigKey(configKey,BasicConfigurationConstants.CONFIG_SOURCES_PREFIX);
				if (cnck != null) {
					// it is a source
					System.out.println(cnck.getComponentName() + "," + cnck.getConfigKey()+","+value);
					if(cnck.getConfigKey().startsWith("interceptors")){
						String interceptor_name = cnck.getConfigKey().substring("interceptors".length());
						System.out.println(interceptor_name+","+ value);
					}
				}

				cnck = parseConfigKey(configKey, BasicConfigurationConstants.CONFIG_CHANNELS_PREFIX);

				if (cnck != null) {
					System.out.println(cnck.getComponentName() + "," + cnck.getConfigKey()+","+value);
				}

				cnck = parseConfigKey(configKey, BasicConfigurationConstants.CONFIG_SINKS_PREFIX);

				if (cnck != null) {
					System.out.println(cnck.getComponentName() + "," + cnck.getConfigKey()+","+value);
				}
				
				

			}

			System.out.println(sources);
			System.out.println(sinks);
			System.out.println(channels);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private ComponentNameAndConfigKey parseConfigKey(String key, String prefix) {
		// key must start with prefix
		if (!key.startsWith(prefix)) {
			return null;
		}

		// key must have a component name part after the prefix of the format:
		// <prefix><component-name>.<config-key>
		int index = key.indexOf('.', prefix.length() + 1);

		if (index == -1) {
			return null;
		}

		String name = key.substring(prefix.length(), index);
		String configKey = key.substring(prefix.length() + name.length() + 1);

		// name and config key must be non-empty
		if (name.length() == 0 || configKey.length() == 0) {
			return null;
		}

		return new ComponentNameAndConfigKey(name, configKey);
	}

	public static class ComponentNameAndConfigKey {

		private final String componentName;
		private final String configKey;

		private ComponentNameAndConfigKey(String name, String configKey) {
			this.componentName = name;
			this.configKey = configKey;
		}

		public String getComponentName() {
			return componentName;
		}

		public String getConfigKey() {
			return configKey;
		}
	}

	public static void main(String[] args) {
		new ConfigTransformer(new File("conf/stream/flume-conf-cassandra.properties")).tranform();
	}
}
