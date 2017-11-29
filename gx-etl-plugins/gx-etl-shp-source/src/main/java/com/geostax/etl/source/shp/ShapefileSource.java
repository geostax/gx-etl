package com.geostax.etl.source.shp;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureIterator;
import org.geotools.geojson.feature.FeatureJSON;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.geostax.etl.ChannelException;
import com.geostax.etl.Context;
import com.geostax.etl.EventDrivenSource;
import com.geostax.etl.conf.Configurable;
import com.geostax.etl.source.AbstractSource;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBWriter;

public class ShapefileSource extends AbstractSource implements EventDrivenSource, Configurable {

	private String shpFileName = "";

	@Override
	public void configure(Context context) {
		shpFileName = context.getString("filename");
	}

	@Override
	public synchronized void start() {
		super.start();
		try {
			ShapefileDataStoreFactory datasoreFactory = new ShapefileDataStoreFactory();
			ShapefileDataStore sds = (ShapefileDataStore) datasoreFactory
					.createDataStore(new File(shpFileName).toURI().toURL());
			sds.setCharset(Charset.forName("GBK"));
			SimpleFeatureSource featureSource = sds.getFeatureSource();
			SimpleFeatureType featureType = featureSource.getFeatures().getSchema();
			SimpleFeatureCollection featureCollection = featureSource.getFeatures();
			FeatureIterator<SimpleFeature> features = featureCollection.features();
			WKBWriter writer = new WKBWriter();
			int count = 0;

			Geometry geom;
			FeatureJSON fjson = new FeatureJSON();

			while (features.hasNext()) {
				StringWriter swriter = new StringWriter();
				SimpleFeature feature = features.next();
				fjson.writeFeature(feature, swriter);
				String json = swriter.toString();
				try {
					InputStream in_nocode = new ByteArrayInputStream(json.getBytes());
					feature = fjson.readFeature(in_nocode);
					in_nocode.close();
					// System.out.println(feature.getFeatureType());
				} catch (Exception e) {
					e.printStackTrace();
				}

				Event event = EventBuilder.withBody(json, Charset.forName("UTF-8"));
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

	@Override
	public synchronized void stop() {
		// TODO Auto-generated method stub
		super.stop();
	}

	public static void main(String[] args) throws Exception {
		ShapefileDataStoreFactory datasoreFactory = new ShapefileDataStoreFactory();
		ShapefileDataStore sds = (ShapefileDataStore) datasoreFactory.createDataStore(
				new File("E:\\Data\\OSM\\Japan\\japan-161212-free.shp\\gis.osm_pois_free_1.shp").toURI().toURL());
		sds.setCharset(Charset.forName("GBK"));
		SimpleFeatureSource featureSource = sds.getFeatureSource();
		SimpleFeatureType featureType = featureSource.getFeatures().getSchema();
		System.out.println(featureType.getName());
		SimpleFeatureCollection featureCollection = featureSource.getFeatures();
		FeatureIterator<SimpleFeature> features = featureCollection.features();
		WKBWriter writer = new WKBWriter();
		int count = 0;

		Geometry geom;
		FeatureJSON fjson = new FeatureJSON();

		while (features.hasNext()) {
			StringWriter swriter = new StringWriter();
			SimpleFeature feature = features.next();
			fjson.writeFeature(feature, swriter);
			String json = swriter.toString();
			try {
				InputStream in_nocode = new ByteArrayInputStream(json.getBytes());
				feature = fjson.readFeature(in_nocode);
				in_nocode.close();
				System.out.println(feature.getFeatureType().getName());
				List<AttributeDescriptor> attrDes = feature.getFeatureType().getAttributeDescriptors();
				for (AttributeDescriptor attr : attrDes) {

					String col_name = attr.getLocalName();
					Class type = attr.getType().getBinding();
					System.out.println(col_name + "," + feature.getAttribute(col_name));

				}
				break;
			} catch (Exception e) {
				e.printStackTrace();
			}

			Event event = EventBuilder.withBody(json, Charset.forName("UTF-8"));
			// process event
			ChannelException ex = null;
		}
	}
	
}
