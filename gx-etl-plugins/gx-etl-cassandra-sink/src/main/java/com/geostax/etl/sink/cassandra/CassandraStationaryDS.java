package com.geostax.etl.sink.cassandra;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.geostax.etl.sink.cassandra.mapper.Catalog;
import com.geostax.etl.sink.cassandra.mapper.Item;
import com.geostax.etl.sink.cassandra.mapper.ItemCodec;
import com.geostax.etl.sink.cassandra.mapper.Observation;
import com.geostax.etl.sink.cassandra.mapper.Observations;

public class CassandraStationaryDS {

	public final static Map<String, DataType> TYPE_TO_CA_MAP = new HashMap<String, DataType>() {
		{
			put("Double", DataType.cdouble());
			put("String", DataType.text());
			put("Map", DataType.map(DataType.text(), DataType.custom("ITEM"), true));
		}
	};
	
	/**
	 * Cache Object
	 */
	private Map<String, Catalog> catalog_cache = new HashMap<>();
	private Map<String, Date> catalog_latest_cache = new HashMap<>();
	
	Session session;
	// Keyspace name
	private String ks = "";

	public CassandraStationaryDS(Session session, String keyspace) {
		this.session = session;
		this.ks = keyspace;
		if (session.getCluster().getMetadata().getKeyspace(keyspace) == null) {
			session.execute("CREATE KEYSPACE " + keyspace
					+ " WITH replication = {'class' : 'SimpleStrategy','replication_factor' : 1};");
			session.execute("use " + keyspace + ";");
		} else {
			session.execute("use " + keyspace + ";");
		}
		
		CodecRegistry codecRegistry=CassandraConnector.getCluster().getConfiguration().getCodecRegistry();
		UserType addressType = CassandraConnector.getCluster().getMetadata().getKeyspace("stream").getUserType("ITEM");
		TypeCodec<UDTValue> addressTypeCodec = codecRegistry.codecFor(addressType);
		ItemCodec addressCodec = new ItemCodec(addressTypeCodec, Item.class);
		codecRegistry.register(addressCodec);

		//initKeyspace();
	}

	/**
	 * Initialized Methods for building cassandra table
	 */

	public void initKeyspace() {

		// Create ITEM type
		String item_type = "CREATE TYPE IF NOT EXISTS ITEM(name text,description text,type text,unit text,maxvalue double,minvalue double);";
		System.out.println(item_type);
		session.execute(item_type);

		// Create Catalog
		String catalog_table = "CREATE TABLE IF NOT EXISTS CATALOG (\n" + "	catalog_id uuid,\n" + "	ks text,\n"
				+ "	feature_schema text,\n" + "	name text,\n" + "	description text,\n" + "	meta text,\n"
				+ "	url text,\n" + "	latest timestamp,\n" + "	items map<text,frozen<item>>,\n"
				+ "	PRIMARY KEY (catalog_id)\n" + ");";

		System.out.println(catalog_table);
		session.execute(catalog_table);

		// Create Catalog Lucene index
		String create_catalog_index = "CREATE CUSTOM INDEX IF NOT EXISTS CATALOG_INDEX ON CATALOG ()\n"
				+ "USING 'com.stratio.cassandra.lucene.Index'\n" + "WITH OPTIONS = {\n" + "   'schema': '{\n"
				+ "      fields: {" + "         name: {type: \"string\"},\n"
				+ "         description: {type: \"text\", analyzer: \"standard\"},\n"
				+ "         meta: {type: \"text\", analyzer: \"standard\"}\n" + "      }\n" + "   }'\n" + "};";
		System.out.println(create_catalog_index);
		session.execute(create_catalog_index);
	}

	public void addCatalog(Catalog catalog) {
		session.execute(
				"INSERT INTO " + ks + "."
						+ " CATALOG(catalog_id,ks,feature_schema,name,description,meta,url,latest,items) VALUES (?,?,?,?,?,?,?,?,?)",
				UUID.randomUUID(), ks, catalog.getFeature_schema(), catalog.getName(), catalog.getDescription(),
				catalog.getMeta(), catalog.getUrl(), catalog.getLatest(), catalog.getItems());

		createDataTable(session, catalog);

	}

	private void createDataTable(Session session, Catalog catalog) {

		String name = catalog.getName();

		String catalog_table = "CREATE TABLE IF NOT EXISTS " + ks + "." + name + " (" + "fid text,\n"
				+ "	bucket text,\n" + "	date timestamp,\n";

		List<String> col_items = new ArrayList<>();
		Map<String, Item> map = catalog.getItems();
		for (Item ob : map.values()) {
			String item_name = ob.getName();
			String item_type = ob.getType();
			col_items.add(item_name + " " + CassandraStationaryDS.TYPE_TO_CA_MAP.get(item_type).getName().toString());
		}
		String cols = "";
		for (int i = 0; i < col_items.size() - 1; i++) {
			cols += col_items.get(i) + ",";
		}
		cols += col_items.get(col_items.size() - 1);
		catalog_table += cols;
		catalog_table += ",PRIMARY KEY ((fid,bucket),date));";
		System.out.println(catalog_table);
		session.execute(catalog_table);
	}
	
	public List<Catalog> getAllCatalog() {
		List<Catalog> result = new ArrayList<>();
		String query = "select * from "+ks+".CATALOG;";
		session.execute("use stream;");
		ResultSet rs = session.execute(query);
		for (Row row : rs) {
			result.add(rowToCatalog(row));
		}
		return result;
	}

	public Catalog getCatalog(UUID id) {
		
		Catalog catalog=null;
		return catalog;
	}

	public Catalog getCatalog(String name) {
		if (catalog_cache.containsKey(name)) {
			return catalog_cache.get(name);
		} else {
			Catalog catalog;
			String catalog_query = "SELECT * FROM  catalog WHERE expr(catalog_index, '{\n"
					+ "   filter: {type: \"phrase\", field: \"name\",value: \"" + name + "\"}\n" + "}');";
			session.execute("use stream;");
			ResultSet rs = session.execute(catalog_query);
			Row row = rs.one();
			if(row==null) return null;
			catalog = rowToCatalog(row);
			catalog_cache.put(name, catalog);
			return catalog;
		}

	}

	private Catalog rowToCatalog(Row row) {
		Catalog catalog=new Catalog();
		catalog.setCatalog_id(row.getUUID("catalog_id"));
		catalog.setDescription(row.getString("description"));
		catalog.setFeature_schema(row.getString("feature_schema"));
		catalog.setKeyspace(row.getString("ks"));
		
		catalog.setItems(row.getMap("items", String.class, Item.class));
		catalog.setMeta(row.getString("meta"));
		catalog.setName(row.getString("name"));
		return catalog;
	}
	
	
	public void addObservation(String catalog_name, Observation observation) {
		Date date = null;
		List values = new ArrayList<>();
		DateFormat df2 = new SimpleDateFormat("yyyy-MM");
		String station_id = observation.getStationid();
		date = observation.getTimestamp();
		String year_month = df2.format(date);
		values.add(station_id);
		values.add(year_month);
		values.add(date);
		Catalog catalog = getCatalog(catalog_name);
		UUID catalog_id = catalog.getCatalog_id();
		Map<String, Item> items = catalog.getItems();
		List<String> keys = new ArrayList(items.keySet());
		Map<String, Object> record = observation.getRecords();
		for (String key : keys) {
			values.add(record.get(key));
		}
		String query = getQuery(catalog.getName(), keys);
		SimpleStatement stat = new SimpleStatement(query, values.toArray());
		session.execute(stat);

		catalog_latest_cache.put(catalog.getName(), date);
		session.execute("INSERT INTO CATALOG (catalog_id,latest) VALUES (?,?)", catalog_id, date);

		System.out.println("Add Observation Finish!");
	}
	/**
	 * Observation methods
	 */

	public void addObservations(String catalog_name, Observations observations) {
		List<Observation> list = observations.getList();
		session.execute("use stream;");
		BatchStatement bs = new BatchStatement();
		DateFormat df2 = new SimpleDateFormat("yyyy-MM");
		Catalog catalog = getCatalog(catalog_name);
		UUID catalog_id = catalog.getCatalog_id();
		Date date = null;
		String table_name = catalog.getName();
		Map<String, Item> items = catalog.getItems();
		List<String> keys = new ArrayList(items.keySet());

		String query = getQuery(table_name, keys);

		for (Observation ob : list) {
			List values = new ArrayList<>();
			String station_id = ob.getStationid();
			date = ob.getTimestamp();
			String year_month = df2.format(date);
			values.add(station_id);
			values.add(year_month);
			values.add(date);
			Map<String, Object> record = ob.getRecords();
			for (String key : keys) {
				values.add(record.get(key));
			}

			SimpleStatement stat = new SimpleStatement(query, values.toArray());
			bs.add(stat);
		}

		session.execute(bs);

		catalog_latest_cache.put(catalog.getName(), date);
		session.execute("INSERT INTO CATALOG (catalog_id,latest) VALUES (?,?)", catalog_id, date);

		System.out.println("Add Observations Finish!");

	}
	
	private String getQuery(String table_name, List<String> keys) {

		StringBuffer builder = new StringBuffer();
		builder.append("INSERT INTO "+ks+"." + table_name + " (fid,bucket,date,");
		String cols = "";
		for (int i = 0; i < keys.size() - 1; i++) {
			cols += keys.get(i) + ",";
		}
		cols += keys.get(keys.size() - 1);
		builder.append(cols + ") VALUES (");
		String temp = "?,?,?,";
		for (int i = 0; i < keys.size(); i++) {
			temp += "?,";
		}
		builder.append(temp.substring(0, temp.length() - 1));
		builder.append(");");
		return builder.toString();

	}
	
	/**
	 * Other complementary methods
	 */

	private List<String> getMonth(Date start, Date end) {
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM");
		List<String> month = new ArrayList<>();
		try {
			Date d1 = sdf2.parse(sdf1.format(start));// 定义起始日期
			Date d2 = sdf2.parse(sdf1.format(end));// 定义结束日期
			Calendar dd = Calendar.getInstance();// 定义日期实例
			dd.setTime(d1);// 设置日期起始时间
			do {// 判断是否到结束日期
				String str = sdf2.format(dd.getTime());
				month.add(str);
				dd.add(Calendar.MONTH, 1);// 进行当前日期月份加1
			} while (dd.getTime().before(end));
			return month;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) throws Exception{

		CodecRegistry codecRegistry = new CodecRegistry();
		Cluster cluster = Cluster.builder().addContactPoint("192.168.210.110").withCredentials("cassandra", "cassandra").withCodecRegistry(codecRegistry)
				.build();
		Session session = cluster.connect();
		CassandraStationaryDS dao = new CassandraStationaryDS(session, "stream");
		dao.initKeyspace();
		
		UserType addressType = cluster.getMetadata().getKeyspace("stream").getUserType("ITEM");
		TypeCodec<UDTValue> addressTypeCodec = codecRegistry.codecFor(addressType);
		ItemCodec addressCodec = new ItemCodec(addressTypeCodec, Item.class);
		codecRegistry.register(addressCodec);
		
	
		//dao.initKeyspace();
		Item item1 = new Item("pfl", "pfl", "Double", "pfl", 0, 0);
		Item item2 = new Item("cod", "cod", "Double", "cod", 0, 0);
		Item item3 = new Item("nh4", "nh4", "Double", "nh4", 0, 0);
		Catalog catalog = new Catalog();
		catalog.setCatalog_id(UUID.randomUUID());
		catalog.setDescription("SewagePlant");
		catalog.setFeature_schema("gis_osm_pois_free_1");
		catalog.setKeyspace("stream");
		catalog.setName("SewagePlant");
		catalog.setUrl("www.jd.com");
		catalog.setMeta("SewagePlant");
		catalog.setLatest(new Date());
		Map<String, Item> items = new HashMap<>();
		items.put(item1.getName(), item1);
		items.put(item2.getName(), item2);
		items.put(item3.getName(), item3);
		catalog.setItems(items);
		dao.addCatalog(catalog);
		
		Thread.sleep(2000);
		
		catalog=dao.getCatalog("SewagePlant");
		System.out.println(catalog);
		
	
		cluster.close();
	}
}
