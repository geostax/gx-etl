package com.geostax.etl.event.observation;

import java.util.Date;
import java.util.Map;
import java.util.UUID;

import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "stream", name = "catalog", caseSensitiveKeyspace = false, caseSensitiveTable = false)
public class Catalog {

	@PartitionKey
	private UUID catalog_id;
	private String namespace;
	private String feature_schema;
	private String name;
	private String description;
	private String meta;
	private String url;
	private Date latest;
	private Map<String, Item> items;

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public String getFeature_schema() {
		return feature_schema;
	}

	public void setFeature_schema(String feature_schema) {
		this.feature_schema = feature_schema;
	}

	public UUID getCatalog_id() {
		return catalog_id;
	}

	public void setCatalog_id(UUID catalog_id) {
		this.catalog_id = catalog_id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getMeta() {
		return meta;
	}

	public void setMeta(String meta) {
		this.meta = meta;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public Date getLatest() {
		return latest;
	}

	public void setLatest(Date latest) {
		this.latest = latest;
	}

	public Map<String, Item> getItems() {
		return items;
	}

	public void setItems(Map<String, Item> items) {
		this.items = items;
	}

}
