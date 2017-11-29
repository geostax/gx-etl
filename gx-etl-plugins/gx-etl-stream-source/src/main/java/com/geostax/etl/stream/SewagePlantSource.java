package com.geostax.etl.stream;

import java.io.File;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.FileUtils;
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
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.geostax.etl.ChannelException;
import com.geostax.etl.Context;
import com.geostax.etl.EventDrivenSource;
import com.geostax.etl.conf.Configurable;
import com.geostax.etl.model.stream.Record;
import com.geostax.etl.source.AbstractSource;

public class SewagePlantSource extends AbstractSource implements EventDrivenSource, Configurable {

	private String address = "";
	private String method = "";
	private String param = "";
	private String name = "";

	@Override
	public void configure(Context context) {
		address = context.getString("address");
		method = context.getString("method");
		param = context.getString("param");
		name = context.getString("name");
	}

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
			List<Record> records = parse(content);
			HashMap<String, String> map = new HashMap<>();
			map.put("table", name);
			for (Record record : records) {
				Event event = EventBuilder.withBody(JSONUtil.toJson(record), Charset.forName("UTF-8"));
				event.setHeaders(map);
				// process event
				ChannelException ex = null;
				try {
					getChannelProcessor().processEvent(event);
				} catch (ChannelException chEx) {
					ex = chEx;
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public List<Record> parse(String content) {
		List<Record> records = new ArrayList<>();
		try {
			// System.out.println(xml);
			FileUtils.writeStringToFile(new File("SewagePlant.xml"), content.replace("utf-8", "utf8"));
			String messageStr = null;
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			DateFormat df2 = new SimpleDateFormat("yyyy-MM");

			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			// Document document = db.parse(new
			// File("xml/SewagePlantRealTimeData.xml"));

			Document document = db.parse(new File("SewagePlant.xml"));

			NodeList list = document.getElementsByTagName("subObject");
			Date current;
			for (int i = 0; i < list.getLength(); i++) {
				// Date current = df.parse("2017-05-24 14:00:00");
				Record record = new Record();

				double codvalue = Double.NaN;
				double NH4value = Double.NaN;
				double pfl = Double.NaN;
				Element element = (Element) list.item(i);

				String subID = element.getElementsByTagName("SubID").item(0).getFirstChild().getNodeValue();
				current = df.parse(element.getElementsByTagName("DateTime").item(0).getFirstChild().getNodeValue());
				record.setDate(current);
				record.setSource_id(subID);
				Map<String, Object> values = new HashMap<>();
				values.put("cod", 0.0);
				values.put("nh4", 0.0);
				values.put("pfl", 0.0);
				if (element.getElementsByTagName("codvalue").item(0).hasChildNodes()) {
					codvalue = Double.parseDouble(
							element.getElementsByTagName("codvalue").item(0).getFirstChild().getNodeValue());
					values.put("cod", codvalue);
				}
				if (element.getElementsByTagName("NH4value").item(0).hasChildNodes()) {
					NH4value = Double.parseDouble(
							element.getElementsByTagName("NH4value").item(0).getFirstChild().getNodeValue());
					values.put("nh4", NH4value);
				}
				if (element.getElementsByTagName("pfl").item(0).hasChildNodes()) {
					pfl = Double
							.parseDouble(element.getElementsByTagName("pfl").item(0).getFirstChild().getNodeValue());
					values.put("pfl", pfl);
				}
				record.setData(values);
				records.add(record);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return records;
	}

}
