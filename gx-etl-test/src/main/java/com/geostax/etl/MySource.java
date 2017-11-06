package com.geostax.etl;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Random;

import org.apache.flume.EventDeliveryException;
import org.apache.flume.event.EventBuilder;

import com.geostax.etl.conf.Configurable;
import com.geostax.etl.source.AbstractSource;

public class MySource extends AbstractSource implements Configurable, PollableSource {

	@Override
	public long getBackOffSleepIncrement() {
		return 0;
	}

	@Override
	public long getMaxBackOffSleepInterval() {
		return 0;
	}

	@Override
	public Status process() throws EventDeliveryException {
		Random random = new Random();
		int randomNum = random.nextInt(100);
		String text = "Hello World :" + random.nextInt(100);
		HashMap<String, String> header = new HashMap<String, String>();
		header.put("id", Integer.toString(randomNum));
		this.getChannelProcessor().processEvent(EventBuilder.withBody(text, Charset.forName("UTF-8"), header));

		return Status.READY;
	}

	@Override
	public void configure(Context arg0) {

	}
}
