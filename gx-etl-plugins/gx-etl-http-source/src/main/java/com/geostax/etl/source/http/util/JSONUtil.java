package com.geostax.etl.source.http.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.google.gson.Gson;

public class JSONUtil {

	/**
	 * 对象转换成json字符串
	 * 
	 * @param obj
	 * @return
	 */
	public static String toJson(Object obj) {
		Gson gson = new Gson();
		return gson.toJson(obj);
	}

	/**
	 * json字符串转成对象
	 * 
	 * @param str
	 * @param type
	 * @return
	 */
	public static <T> T fromJson(String str, Type type) {
		Gson gson = new Gson();
		return gson.fromJson(str, type);
	}

	/**
	 * json字符串转成对象
	 * 
	 * @param str
	 * @param type
	 * @return
	 */
	public static <T> T fromJson(String str, Class<T> type) {
		Gson gson = new Gson();
		return gson.fromJson(str, type);
	}

	/**
	 * json字符串转成对象
	 * 
	 * @param str
	 * @param type
	 * @return
	 */
	public static <T> T fromJson(Reader reader, Class<T> type) {
		Gson gson = new Gson();
		return gson.fromJson(reader, type);
	}

	public static void main(String[] args) throws Exception {
		File file = new File("C:\\Users\\phil\\Desktop\\sewage.log");
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = "";
		int i = 1;
		while ((line = reader.readLine()) != null) {
			// System.out.println(line);
			try {
				Data data = JSONUtil.fromJson(line, Data.class);
				i++;
				System.out.println(data.getRaw());
			} catch (Exception e) {
				System.out.println(i + ":" + line);
			}

		}

	}

	class Data {
		String url;
		String raw;

		public Data() {
			// TODO Auto-generated constructor stub
		}

		public String getUrl() {
			return url;
		}

		public void setUrl(String url) {
			this.url = url;
		}

		public String getRaw() {
			return raw;
		}

		public void setRaw(String raw) {
			this.raw = raw;
		}

	}
}
