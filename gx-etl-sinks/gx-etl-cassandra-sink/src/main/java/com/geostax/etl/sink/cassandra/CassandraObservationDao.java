package com.geostax.etl.sink.cassandra;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.geostax.etl.event.observation.Catalog;
import com.geostax.etl.event.observation.Item;
import com.geostax.etl.event.observation.Record;

public class CassandraObservationDao {

	public final static Map<String, DataType> TYPE_TO_CA_MAP = new HashMap<String, DataType>() {
		{
			put("Double", DataType.cdouble());
			put("String", DataType.text());
		}
	};

	private Session session;
	private String keyspace;

	public CassandraObservationDao(Session session, String keyspace) {
		this.session = session;
		this.keyspace = keyspace;
		if (session.getCluster().getMetadata().getKeyspace(keyspace) == null) {
			session.execute("CREATE KEYSPACE " + keyspace
					+ " WITH replication = {'class' : 'SimpleStrategy','replication_factor' : 1};");
			session.execute("use " + keyspace + ";");
		} else {
			session.execute("use " + keyspace + ";");
		}

		initKeyspace();
	}

	public CassandraObservationDao(String keyspace) {
		this.keyspace = keyspace;
		session = CassandraConnector.getSession();

		if (session.getCluster().getMetadata().getKeyspace(keyspace) == null) {
			session.execute("CREATE KEYSPACE " + keyspace
					+ " WITH replication = {'class' : 'SimpleStrategy','replication_factor' : 1};");
			session.execute("use " + keyspace + ";");
		} else {
			session.execute("use " + keyspace + ";");
		}

		initKeyspace();
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
		session.execute("use " + keyspace + ";");
		initKeyspace();
	}

	public void initKeyspace() {

		// Create ITEM type
		String item_type = "CREATE TYPE IF NOT EXISTS ITEM(name text,description text,type text,unit text,maxvalue double,minvalue double);";
		System.out.println(item_type);
		session.execute(item_type);

		// Create Catalog
		String catalog_table = "CREATE TABLE IF NOT EXISTS CATALOG (\n" + "	catalog_id uuid,\n" + "	namespace text,\n"
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

	public void registerCatalog(Catalog catalog) {

		createDataTable(catalog);
	}

	private void createDataTable(Catalog catalog) {

		String name = catalog.getName();

		String catalog_table = "CREATE TABLE IF NOT EXISTS  " + keyspace + "." + name + " (" + "feature_id text,\n"
				+ "	bucket text,\n" + "	date timestamp,\n";

		List<String> col_items = new ArrayList<>();
		Map<String, Item> map = catalog.getItems();
		for (Item ob : map.values()) {
			String item_name = ob.getName();
			String item_type = ob.getType();
			col_items.add(item_name + " " + TYPE_TO_CA_MAP.get(item_type).getName().toString());
		}
		String cols = "";
		for (int i = 0; i < col_items.size() - 1; i++) {
			cols += col_items.get(i) + ",";
		}
		cols += col_items.get(col_items.size() - 1);
		catalog_table += cols;
		catalog_table += ",PRIMARY KEY ((feature_id,bucket),date));";
		System.out.println(catalog_table);
		session.execute(catalog_table);
	}

	public void removeCatalog(String id) {

	}

	public Catalog getCatalog(String id) {
		return null;
	}

	public void addRecord(String catalog_id, Record record) {

	}

	public void getRecord() {

	}

	public static void main(String[] args) {
		Cluster cluster = Cluster.builder().addContactPoint("192.168.210.110").withCredentials("cassandra", "cassandra")
				.build();
		Session session = cluster.connect();
		CassandraObservationDao dao=new CassandraObservationDao(session, "stream1"); 
		dao.initKeyspace();
		Item item=new Item("item1", "item1", "Double", "item1", 0, 0);
		Catalog catalog=new Catalog();
		catalog.setCatalog_id(UUID.randomUUID());
		catalog.setDescription("catalog1");
		catalog.setFeature_schema("gis_osm_pois_free_1");
		catalog.setNamespace("stream1");
		catalog.setName("wastegas");
		catalog.setUrl("www.jd.com");
		catalog.setMeta("ssss");
		catalog.setLatest(new Date());
		Map<String,Item> items=new HashMap<>();
		items.put(item.getName(),item);
		catalog.setItems(items);
		dao.registerCatalog(catalog);
		
		
		cluster.close();

	}
}
