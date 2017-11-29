package com.geostax.etl.flow1;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.geostax.etl.node.Application;
import com.geostax.etl.node.PropertiesFileConfigurationProvider;
import com.google.common.io.Files;

public class MyAgent3 {

	public static void main(String[] args) throws Exception {
		Application application = null;
		File configurationFile = new File("conf/flow1/flume-conf-cassandra.properties");
		PropertiesFileConfigurationProvider configurationProvider = new PropertiesFileConfigurationProvider("a3",configurationFile);
		application = new Application();
		application.handleConfigurationEvent(configurationProvider.getConfiguration());
		application.start();
	}
}
