package com.geostax.etl.sink.cassandra;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.geostax.etl.Channel;
import com.geostax.etl.Context;
import com.geostax.etl.Transaction;
import com.geostax.etl.conf.Configurable;
import com.geostax.etl.event.EventHelper;
import com.geostax.etl.model.stream.Record;
import com.geostax.etl.sink.AbstractSink;
import com.geostax.etl.sink.cassandra.mapper.Catalog;
import com.geostax.etl.sink.cassandra.mapper.Item;
import com.geostax.etl.sink.cassandra.mapper.Observation;
import com.geostax.etl.sink.cassandra.util.JSONUtil;

public class CassandraStationarySink extends AbstractSink implements Configurable {

	// Default Max bytes to dump
	public static final int DEFAULT_MAX_BYTE_DUMP = 16;

	// Max number of bytes to be dumped
	private int maxBytesToLog = DEFAULT_MAX_BYTE_DUMP;

	CassandraStationaryDS datastore=null;
			
	public static final String HOST = "host";
	public static final String PORT = "port";
	public static final String USERNAME = "user";
	public static final String PASSWORD = "password";
	public static final String KEYSPACE = "keyspace";
	public static final String TABLE = "table";
	//public static final String CQL = "cql";

	@Override
	public void configure(Context context) {
		String host = context.getString(HOST);
		String port = context.getString(PORT);
		String user = context.getString(USERNAME);
		String password = context.getString(PASSWORD);
		String keyspace=context.getString(KEYSPACE);
		String table=context.getString(TABLE);
		
		datastore=new CassandraStationaryDS(CassandraConnector.getSession(), keyspace);
				
		//String cql = context.getString(CQL);
	}

	@Override
	public synchronized void start() {

		super.start();
		
		
	}

	@Override
	public synchronized void stop() {
		// TODO Auto-generated method stub
		super.stop();
	}

	@Override
	public Status process() throws EventDeliveryException {
		Status result = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		Event event = null;

		try {
			transaction.begin();
			event = channel.take();

			if (event != null) {
				String table_name=event.getHeaders().get("table");
				String content=new String(event.getBody());
				Record record=JSONUtil.fromJson(content, Record.class);
				Observation ob=new Observation();
				ob.setRecords(record.getData());
				ob.setTimestamp(record.getDate());
				ob.setStationid(record.getSource_id());
				datastore.addObservation(table_name, ob);
				System.out.println(content);
				
			} else {
				// No event found, request back-off semantics from the sink
				// runner
				result = Status.BACKOFF;
			}
			transaction.commit();
		} catch (Exception ex) {
			transaction.rollback();
			throw new EventDeliveryException("Failed to log event: " + event, ex);
		} finally {
			transaction.close();
		}

		return result;
	}

	public static void main(String[] args) {
		Cluster cluster = Cluster.builder().addContactPoint("192.168.210.110").withCredentials("cassandra", "cassandra")
				.build();
		Session session = cluster.connect();
		CassandraStationaryDS dao=new CassandraStationaryDS(session, "stream1"); 
		dao.initKeyspace();
		Item item=new Item("item1", "item1", "Double", "item1", 0, 0);
		Catalog catalog=new Catalog();
		catalog.setCatalog_id(UUID.randomUUID());
		catalog.setDescription("catalog1");
		catalog.setFeature_schema("gis_osm_pois_free_1");
		catalog.setKeyspace("stream1");
		catalog.setName("wastegas");
		catalog.setUrl("www.jd.com");
		catalog.setMeta("ssss");
		catalog.setLatest(new Date());
		Map<String,Item> items=new HashMap<>();
		items.put(item.getName(),item);
		catalog.setItems(items);
		dao.addCatalog(catalog);
		
		
		cluster.close();

	}
}
