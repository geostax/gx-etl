package com.geostax.etl.source.http;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import com.geostax.etl.ChannelException;
import com.geostax.etl.Context;
import com.geostax.etl.EventDrivenSource;
import com.geostax.etl.conf.Configurable;
import com.geostax.etl.source.AbstractSource;

public class HttpGetSource extends AbstractSource implements EventDrivenSource, Configurable {

	private String address = "";
	private String method = "";
	private String param = "";
	private String name = "";
	private String format = "";

	@Override
	public void configure(Context context) {
		address = context.getString("address");
		method = context.getString("method");
		param = context.getString("param");
		name = context.getString("name");
		format = context.getString("format");
		
	}

	@Override
	public synchronized void start() {

		try {
			CloseableHttpClient httpClient = HttpClients.createDefault();
			CloseableHttpResponse response = null;
			if (method.equals("GET")) {
				HttpGet httpget = new HttpGet(address);
				response = httpClient.execute(httpget);
			} else if (method.equals("POST")) {
				String[] params = param.split(",");
				List<NameValuePair> nvps = new ArrayList<NameValuePair>();
				for (int i = 3; i < params.length; i++) {
					String items[] = params[i].split("=");
					nvps.add(new BasicNameValuePair(items[0], items[1]));
				}
				HttpPost httpPost = new HttpPost(address);
				httpPost.setEntity(new UrlEncodedFormEntity(nvps, "utf-8"));
				response = httpClient.execute(httpPost);
			}

			HttpEntity httpEntity = response.getEntity();
			String content = EntityUtils.toString(httpEntity);
			HashMap<String, String> map=new HashMap<>();
			map.put("name",name);
			map.put("format",format);
			Event event = EventBuilder.withBody(content, Charset.forName("UTF-8"));
			event.setHeaders(map);
			
			// process event
			ChannelException ex = null;
			try {
				getChannelProcessor().processEvent(event);
			} catch (ChannelException chEx) {
				ex = chEx;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private void parse(String content) {

		if (format.equals("json")) {
			parseJSON(content);
		} else if (format.equals("xml")) {

		}
	}

	private void parseJSON(String content) {

	}

	private void parseXML() {

	}

	@Override
	public synchronized void stop() {
		// TODO Auto-generated method stub
		super.stop();
	}
}
