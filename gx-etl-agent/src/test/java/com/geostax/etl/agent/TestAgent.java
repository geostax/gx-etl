package com.geostax.etl.agent;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class TestAgent {
	private static final Logger LOGGER = LoggerFactory.getLogger(TestAgent.class);
	private static final String HOSTNAME = "localhost";
	private static AtomicInteger serialNumber = new AtomicInteger(0);
	private EmbeddedAgent agent;
	private Map<String, String> properties;
	private EventCollector eventCollector;
	private NettyServer nettyServer;
	private Map<String, String> headers;
	private byte[] body;

	public void text() throws Exception {
		headers = Maps.newHashMap();
		headers.put("key1", "value1");
		body = "body".getBytes(Charsets.UTF_8);

		int port = findFreePort();
		eventCollector = new EventCollector();
		Responder responder = new SpecificResponder(AvroSourceProtocol.class, eventCollector);
		nettyServer = new NettyServer(responder, new InetSocketAddress(HOSTNAME, port));
		nettyServer.start();

		// give the server a second to start
		Thread.sleep(1000L);

		properties = Maps.newHashMap();
		properties.put("source.type", EmbeddedAgentConfiguration.SOURCE_TYPE_EMBEDDED);
		properties.put("channel.type", "memory");
		properties.put("channel.capacity", "200");
		properties.put("sinks", "sink1 sink2");
		properties.put("sink1.type", "avro");
		properties.put("sink2.type", "avro");
		properties.put("sink1.hostname", HOSTNAME);
		properties.put("sink1.port", String.valueOf(port));
		properties.put("sink2.hostname", HOSTNAME);
		properties.put("sink2.port", String.valueOf(port));
		properties.put("processor.type", "load_balance");
		properties.put("source.interceptors", "i1");
		properties.put("source.interceptors.i1.type", "static");
		properties.put("source.interceptors.i1.key", "key2");
		properties.put("source.interceptors.i1.value", "value2");
		
		agent = new EmbeddedAgent("test-" + serialNumber.incrementAndGet());
		agent.configure(properties);
		agent.start();
		
		testPut();
		
		for(int i=0;i<10;i++) {
			System.out.println("...");
			Thread.sleep(1000);
		}
		testPut();
	}

	public void testPut() throws Exception {
		
		agent.put(EventBuilder.withBody(body, headers));
		Event event;
		while ((event = eventCollector.poll()) == null) {
			Thread.sleep(500L);
		}
		System.out.println(event.getHeaders());
		System.out.println(new String(event.getBody()));

	}

	class EventCollector implements AvroSourceProtocol {
		private final Queue<AvroFlumeEvent> eventQueue = new LinkedBlockingQueue<AvroFlumeEvent>();

		public Event poll() {
			AvroFlumeEvent avroEvent = eventQueue.poll();
			if (avroEvent != null) {
				return EventBuilder.withBody(avroEvent.getBody().array(), toStringMap(avroEvent.getHeaders()));
			}
			return null;
		}

		@Override
		public Status append(AvroFlumeEvent event) throws AvroRemoteException {
			eventQueue.add(event);
			return Status.OK;
		}

		@Override
		public Status appendBatch(List<AvroFlumeEvent> events) throws AvroRemoteException {
			Preconditions.checkState(eventQueue.addAll(events));
			return Status.OK;
		}
	}

	private Map<String, String> toStringMap(Map<CharSequence, CharSequence> charSeqMap) {
		Map<String, String> stringMap = new HashMap<String, String>();
		for (Map.Entry<CharSequence, CharSequence> entry : charSeqMap.entrySet()) {
			stringMap.put(entry.getKey().toString(), entry.getValue().toString());
		}
		return stringMap;
	}

	private int findFreePort() throws IOException {
		try (ServerSocket socket = new ServerSocket(0)) {
			return socket.getLocalPort();
		}
	}

	public static void main(String[] args) throws Exception {
		new TestAgent().text();
	}
}
