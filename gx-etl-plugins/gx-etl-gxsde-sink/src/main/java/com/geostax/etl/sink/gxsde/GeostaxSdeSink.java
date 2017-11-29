package com.geostax.etl.sink.gxsde;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.geotools.data.DataUtilities;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geojson.feature.FeatureJSON;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.geostax.cassandra.CassandraDataStore;
import com.geostax.cassandra.CassandraDataStoreFactory;
import com.geostax.cassandra.CassandraFeatureStore;
import com.geostax.etl.Channel;
import com.geostax.etl.Context;
import com.geostax.etl.Transaction;
import com.geostax.etl.conf.Configurable;
import com.geostax.etl.sink.AbstractSink;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.WKBWriter;

public class GeostaxSdeSink extends AbstractSink implements Configurable {

	FeatureJSON fjson = new FeatureJSON();
	List<SimpleFeature> buffer = new ArrayList<>();
	WKBWriter writer = new WKBWriter();

	@Override
	public void configure(Context context) {

	}

	@Override
	public Status process() throws EventDeliveryException {
		Status result = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		Event event = null;

		Map<String, Serializable> params = new HashMap<>();
		CassandraDataStoreFactory spi = new CassandraDataStoreFactory();
		params.put(CassandraDataStoreFactory.DBTYPE.key, "cassandra");
		params.put(CassandraDataStoreFactory.HOST.key, "192.168.210.110");
		params.put(CassandraDataStoreFactory.USER.key, "cassandra");
		params.put(CassandraDataStoreFactory.PASSWD.key, "cassandra");

		try {
			CassandraDataStore datastore = (CassandraDataStore) spi.createDataStore(params);
			SimpleFeatureSource cfeatureSource = datastore.getFeatureSource(datastore.getTypeNames()[0]);
			CassandraFeatureStore cfeatureStore = (CassandraFeatureStore) cfeatureSource;
			transaction.begin();
			event = channel.take();

			 SimpleFeatureType TYPE = DataUtilities.createType("Location",
						"the_geom:Point:srid=4326," + // <- the geometry attribute:
														// Point type
								"name:String," + // <- a String attribute
								"osm_id:String,"+ // a number attribute
								"code:Integer,"+
								"fclass:String"
				);
			 SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(TYPE);
			
			 
			if (event != null) {
				String content = new String(event.getBody());
				SimpleFeature feature = fjson.readFeature(content);
				buffer.add(convert(feature,featureBuilder));
				System.out.println(feature.getAttribute("osm_id"));
				if (buffer.size() == 10) {
					cfeatureStore.addFeatures(buffer);
					
					buffer.clear();
				}
			} else {
				// No event found, request back-off semantics from the sink
				// runner
				result = Status.BACKOFF;
			}
			transaction.commit();
		} catch (Exception ex) {
			transaction.rollback();
			throw new EventDeliveryException("Failed to log event: " + event, ex);
		} finally {
			transaction.close();
		}

		return result;
	}

	public SimpleFeature convert(SimpleFeature feature,SimpleFeatureBuilder builder) {
		 /* Longitude (= x coord) first ! */
		//System.out.println(feature);
        Point point = (Point)feature.getDefaultGeometry();
        builder.add(point);
        builder.add(feature.getAttribute("name"));
        builder.add(feature.getAttribute("osm_id"));
        builder.add(feature.getAttribute("code"));
        builder.add(feature.getAttribute("fclass"));
        return builder.buildFeature(feature.getAttribute("osm_id").toString());
	}
}
