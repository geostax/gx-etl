package com.geostax.etl.http;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

public class AirQualityHandler2 {

	public AirQualityHandler2() {
		// TODO Auto-generated constructor stub
	}

	public void execute() throws Exception {

		String[] params = new String[] { "http://122.4.213.20:9000/Public/AirQualityWebServiceDemo.aspx" };
		Date date = new Date();
		date = new Date(date.getTime() - 3600000);
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		// System.out.println(df.format(date));

		CloseableHttpClient httpClient = HttpClients.createDefault();
		// http://10.94.64.2:8181/Public/AirQualityWebServiceDemo.aspx
		// "http://122.4.213.20:9000/Public/AirQualityWebServiceDemo.aspx"
		HttpPost httpPost = new HttpPost(params[0]);

		List<NameValuePair> nvps = new ArrayList<NameValuePair>();
		nvps.add(new BasicNameValuePair("__VIEWSTATE",
				"/wEPDwULLTE3MDMzMDI0MjlkZCtL32rdfkL1CFo06P2oldkwhjdspEHlRQ9fBtKsQtgq"));
		nvps.add(new BasicNameValuePair("__EVENTVALIDATION",
				"/wEWBwLg1ZuIDgKDovrKAQKJh9T0DQKqnYfVBgLmgNSuBQKy4IHTCwLnhqXoCPbsTc7UuDElunSKqecML9NQpjVIAWS8AWg0PC7LmKKP"));

		nvps.add(new BasicNameValuePair("textBoxUserId", "web"));
		nvps.add(new BasicNameValuePair("textBoxPassword", "web"));
		nvps.add(new BasicNameValuePair("textBoxDataTime", df.format(date)));
		nvps.add(new BasicNameValuePair("textBoxPollutantNames", ""));
		nvps.add(new BasicNameValuePair("buttonGetHourlyData", "调用"));
		// nvps.add(new BasicNameValuePair("buttonGetHourlyDataCode", "C#代码"));

		httpPost.setEntity(new UrlEncodedFormEntity(nvps, "utf8"));
		CloseableHttpResponse response = httpClient.execute(httpPost);
		HttpEntity httpEntity = response.getEntity();
		String strResult = EntityUtils.toString(httpEntity);
		System.out.println(strResult);

		System.out.println("Sucess");

	}

	public static void main(String[] args) throws Exception {
		new AirQualityHandler2().execute();
	}
}
