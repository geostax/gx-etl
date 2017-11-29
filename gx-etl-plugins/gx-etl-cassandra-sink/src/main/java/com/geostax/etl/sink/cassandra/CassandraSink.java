package com.geostax.etl.sink.cassandra;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.geostax.etl.Channel;
import com.geostax.etl.Context;
import com.geostax.etl.Transaction;
import com.geostax.etl.Sink.Status;
import com.geostax.etl.conf.Configurable;
import com.geostax.etl.event.EventHelper;
import com.geostax.etl.sink.AbstractSink;

public class CassandraSink extends AbstractSink implements Configurable {

	private static final Logger logger = LoggerFactory.getLogger(CassandraSink.class);

	// Default Max bytes to dump
	public static final int DEFAULT_MAX_BYTE_DUMP = 16;

	// Max number of bytes to be dumped
	private int maxBytesToLog = DEFAULT_MAX_BYTE_DUMP;

	public static final String HOST = "host";
	public static final String PORT = "port";
	public static final String USERNAME = "user";
	public static final String PASSWORD = "password";

	public static final String KEYSPACE = "keyspace";
	public static final String TABLE = "table";

	public static final String CQL = "cql";

	@Override
	public void configure(Context context) {
		String host = context.getString(HOST);
		String port = context.getString(PORT);
		String user = context.getString(USERNAME);
		String password = context.getString(PASSWORD);
		String cql = context.getString(CQL);
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

				String content = new String(event.getBody());
				System.out.println("===>" + content);

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

}
