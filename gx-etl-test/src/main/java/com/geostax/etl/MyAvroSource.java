package com.geostax.etl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.geostax.etl.channel.ChannelProcessor;
import com.geostax.etl.channel.MemoryChannel;
import com.geostax.etl.channel.ReplicatingChannelSelector;
import com.geostax.etl.conf.Configurables;
import com.geostax.etl.source.AvroSource;

public class MyAvroSource {

	private AvroSource source;
	private Channel channel;
	private InetAddress localhost;

	public void setUp() throws UnknownHostException {
		localhost = InetAddress.getByName("127.0.0.1");
		source = new AvroSource();
		channel = new MemoryChannel();

		Configurables.configure(channel, new Context());

		List<Channel> channels = new ArrayList<Channel>();
		channels.add(channel);

		ChannelSelector rcs = new ReplicatingChannelSelector();
		rcs.setChannels(channels);

		source.setChannelProcessor(new ChannelProcessor(rcs));
		
	    Context context = new Context();

	    context.put("port", "41414");
	    context.put("bind", "0.0.0.0");

	    Configurables.configure(source, context);

	    source.start();
	}
	
	public static void main(String[] args) throws Exception {
		new MyAvroSource().setUp();
		
	}

}
