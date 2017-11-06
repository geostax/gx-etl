package com.geostax.etl;

import org.apache.flume.Event;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

import com.google.common.base.Charsets;

public class MyClient {
	private static final int BATCH_SIZE = 5;
	public static void main(String[] args) throws Exception {
		String hostname="localhost";
		int port=55555;
		RpcClient rpcClient = RpcClientFactory.getDefaultInstance(hostname, port, BATCH_SIZE);
		
		while(true){
			String line="new event";
			Event event=EventBuilder.withBody(line, Charsets.UTF_8);
			rpcClient.append(event);
			Thread.sleep(5000);
		}
	}

}
