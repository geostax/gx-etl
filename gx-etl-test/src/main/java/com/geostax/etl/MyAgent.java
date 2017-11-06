package com.geostax.etl;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.geostax.etl.node.Application;
import com.geostax.etl.node.PropertiesFileConfigurationProvider;
import com.google.common.io.Files;

public class MyAgent {

	public static void main(String[] args) throws Exception {
		Application application = null;
		File configurationFile = new File("flume-conf-shp.properties");
		PropertiesFileConfigurationProvider configurationProvider = new PropertiesFileConfigurationProvider("a1",configurationFile);
		application = new Application();
		application.handleConfigurationEvent(configurationProvider.getConfiguration());
		application.start();
	}
}
