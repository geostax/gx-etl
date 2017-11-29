package com.geostax.etl.nmstream;

import java.io.File;

import com.geostax.etl.node.Application;
import com.geostax.etl.node.PropertiesFileConfigurationProvider;

public class NMStreamAgent {
	public static void main(String[] args) throws Exception {
		Application application = null;
		File configurationFile = new File("conf/nmstream/flume-conf-nmstream.properties");
		PropertiesFileConfigurationProvider configurationProvider = new PropertiesFileConfigurationProvider("a1",
				configurationFile);
		application = new Application();
		application.handleConfigurationEvent(configurationProvider.getConfiguration());
		application.start();
	}
}
