package com.geostax.etl.sink.cassandra;

import java.io.InputStream;
import java.util.Properties;
import java.util.PropertyResourceBundle;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Session;

/**
 * This class applies the Singleton pattern to improve the Cassandra connection
 **/
public class CassandraConnector {

	private static Cluster cluster = null;

	static {
		try {
			Properties prop = new Properties();
			InputStream in = Object.class.getResourceAsStream("config.properties");
			prop.load(in);
			String[] hosts = prop.getProperty("hosts").split(",");
			Builder builder = Cluster.builder();
			for (String host : hosts) {
				builder = builder.addContactPoint(host);
			}
			cluster = builder.build();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Cluster getCluster() {
		return cluster;
	}

	public static Session getSession() {
		return cluster.connect();
	}

	public static void close() {
		cluster.close();
	}
}
