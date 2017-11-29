package com.geostax.etl.http;

import java.io.File;
import java.util.Properties;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;


public class StreamHttpGetHandler {
	

	public void execute() throws Exception {
		// TODO Auto-generated method stub
		String url ="http://122.4.213.20:8407/GuTu_webservice/WebService/InterfaceForWF/DataServiceFor.asmx/SewagePlantRealTimeData";	
		CloseableHttpClient httpClient = HttpClients.createDefault();
		HttpGet httpget = new HttpGet(url);
		CloseableHttpResponse response = httpClient.execute(httpget);
		HttpEntity httpEntity = response.getEntity();
		String content = EntityUtils.toString(httpEntity);
		
		//String content = FileUtils.getContentsAsString(new File("xml/AirRealTimeData.xml"));
		System.out.println(content);

	}
	
	public static void main(String[] args) throws Exception{
		//new StreamHttpGetHandler().execute("10.88.40.185:9093", "SewagePlant",new String[]{"http://10.94.64.2:8181/InterfaceForWF/DataServiceFor/SewagePlantRealTimeData"});
		//new StreamHttpGetHandler().execute("localhost:9092", "SewagePlant",
		//		new String[]{"http://10.94.64.2:8181/InterfaceForWF/DataServiceFor/SewagePlantRealTimeData"});
		
		//new StreamHttpGetHandler().execute("localhost:9092", "WasteGas",
		//		new String[]{"http://10.94.64.2:8181/InterfaceForWF/DataServiceFor/WasteGasRealTimeData"});

		//new StreamHttpGetHandler().execute("localhost:9092", "WasteWater",
		//		new String[]{"http://122.4.213.20:8407/GuTu_webservice/WebService/InterfaceForWF/DataServiceFor.asmx/WasteWaterRealTimeData"});
		
		new StreamHttpGetHandler().execute();

	}
}
