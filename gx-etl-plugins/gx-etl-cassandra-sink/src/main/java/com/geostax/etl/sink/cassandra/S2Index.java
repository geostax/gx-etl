package com.geostax.etl.sink.cassandra;

import java.util.ArrayList;
import java.util.List;

import org.geotools.data.DataUtilities;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.geometry.S2Cell;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Loop;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polygon;
import com.google.common.geometry.S2Polyline;
import com.google.common.geometry.S2RegionCoverer;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTReader;

public class S2Index {

	String tableName;
	public final static int QUAD_LEVEL = 10;
	S2RegionCoverer coverer;

	public S2Index() {
		this.coverer = new S2RegionCoverer();
	}

	public List<S2CellId> index(int level, Geometry geom) {
		ArrayList<S2CellId> covering = new ArrayList<>();
		coverer.setMaxLevel(level);
		coverer.setMinLevel(level);
		S2CellId id = null;
		if (geom instanceof Point) {
			Point point = (Point) geom;
			double x = point.getX();
			double y = point.getY();
			id = new S2Cell(S2LatLng.fromDegrees(y, x)).id();
			covering.add(id);
		} else if (geom instanceof MultiLineString) {
			List<S2Point> vertices = Lists.newArrayList();
			Coordinate[] coordinates = geom.getCoordinates();
			for (Coordinate coord : coordinates) {
				vertices.add(S2LatLng.fromDegrees(coord.y, coord.x).toPoint());
			}
			S2Polyline line = new S2Polyline(vertices);
			coverer.getCovering(line, covering);
		} else if (geom instanceof MultiPolygon) {
			MultiPolygon multip = (MultiPolygon) geom;
			int count = multip.getNumGeometries();
			for (int i = 0; i < count; i++) {
				Geometry sub_geom = multip.getGeometryN(i);
				Polygon p = (Polygon) sub_geom;
				LineString ring = p.getExteriorRing();
				Coordinate[] coordinates = ring.getCoordinates();
				StringBuilder sb = new StringBuilder();

				for (Coordinate coord : coordinates) {
					sb.append(coord.y + ":" + coord.x + ",");
				}
				sb.replace(sb.length() - 1, sb.length(), ";");
				S2Polygon polygon = makePolygon(sb.toString());
				ArrayList<S2CellId> covering2 = new ArrayList<>();
				coverer.getCovering(polygon, covering2);
				covering.addAll(covering2);
			}

		}
		return covering;

	}

	public static void main(String[] args) throws Exception {
		WKTReader wktReader = new WKTReader(new GeometryFactory());
		final SimpleFeatureType TYPE = DataUtilities.createType("Geom",
				"the_geom:MultiPolygon:srid=4326," + // <- the geometry
														// attribute: Point type
						"name:String," + // <- a String attribute
						"number:Integer" // a number attribute
		);

		SimpleFeature feature = SimpleFeatureBuilder.build(TYPE,
				new Object[] { wktReader.read("MULTIPOLYGON(((135 40, 136 40,136 39, 135 39, 135 40)))"), "name1" },
				null);
		Geometry geom = wktReader.read(
				"MULTIPOLYGON (((-79.0446364 35.9358774, -79.0444788 35.9359122, -79.04447 35.9359142, -79.0444318 35.9359227, -79.0444331 35.9359267, -79.0444531 35.9359862, -79.0444558 35.9359943, -79.0444609 35.9360093, -79.0444753 35.9360055, -79.0444818 35.9360038, -79.0444853 35.9360122, -79.044491 35.936029, -79.044464 35.936035, -79.0443712 35.9360557, -79.0443111 35.9360691, -79.0441955 35.9360948, -79.0440555 35.936126, -79.0439321 35.9361535, -79.0439197 35.9361563, -79.0439131 35.9361578, -79.0438215 35.9361782, -79.0437866 35.9361859, -79.0437467 35.9361948, -79.0437534 35.9362145, -79.0437671 35.9362552, -79.0437996 35.9363518, -79.0437982 35.9363552, -79.0437592 35.936408, -79.043734 35.9364136, -79.0437364 35.9364242, -79.0437464 35.9364539, -79.0437476 35.9364573, -79.0437594 35.9364925, -79.0437556 35.9364933, -79.0437484 35.9364949, -79.0437214 35.9365009, -79.0437072 35.9365041, -79.043655 35.9365157, -79.0436188 35.9365238, -79.043483 35.936554, -79.0434133 35.9365695, -79.043409 35.936557, -79.0433632 35.9364209, -79.0433333 35.9363321, -79.0433176 35.9362853, -79.0433096 35.9362616, -79.0433313 35.9362567, -79.0434476 35.9362308, -79.0434497 35.936237, -79.0434605 35.936269, -79.0434974 35.9362608, -79.043491 35.9362419, -79.0434889 35.9362357, -79.0434961 35.9362341, -79.0434935 35.9362265, -79.0434885 35.9362116, -79.0434747 35.9361707, -79.0434736 35.9361672, -79.0434582 35.9361707, -79.0433623 35.936192, -79.0433168 35.9362022, -79.0432949 35.936207, -79.043289 35.936208, -79.0432919 35.9362066, -79.0432851 35.9361867, -79.0432898 35.9361856, -79.0432986 35.9361837, -79.043318 35.9361794, -79.0433438 35.9361736, -79.0433427 35.9361702, -79.0433384 35.9361574, -79.0433339 35.9361441, -79.0432809 35.9361559, -79.0432733 35.9361576, -79.0432733 35.9361573, -79.0432699 35.9361474, -79.0432325 35.9360363, -79.0432659 35.9360288, -79.0432839 35.9360248, -79.0432945 35.9360225, -79.0433189 35.9360171, -79.043395 35.9360001, -79.0434148 35.9359957, -79.0434306 35.9359922, -79.0433528 35.935761, -79.0432366 35.9357868, -79.0432307 35.935772, -79.0432283 35.9357647, -79.0432244 35.9357531, -79.0432694 35.9357431, -79.0433515 35.9357248, -79.0433497 35.9357195, -79.0433416 35.9356954, -79.0433266 35.9356987, -79.0432977 35.9357051, -79.0432634 35.9357128, -79.0432468 35.9357165, -79.0432385 35.9357183, -79.0432378 35.9357162, -79.0432353 35.9357087, -79.0432139 35.9356452, -79.0432085 35.9356292, -79.043206 35.9356216, -79.0432155 35.9356195, -79.0432283 35.9356166, -79.0432671 35.935608, -79.0433106 35.9355983, -79.0433082 35.9355912, -79.043347 35.9355826, -79.0434037 35.93557, -79.0434203 35.9355663, -79.0434268 35.9355856, -79.0434309 35.9355977, -79.0434358 35.9356121, -79.0434515 35.9356588, -79.043506 35.9356467, -79.0436041 35.9356248, -79.0436301 35.935619, -79.0436577 35.9356129, -79.0436478 35.9355835, -79.0436439 35.9355719, -79.0436886 35.9355814, -79.0437 35.9355837, -79.043701 35.9355807, -79.0437307 35.9354923, -79.0437326 35.9354865, -79.0437345 35.935481, -79.0437395 35.935466, -79.0437437 35.9354669, -79.0437605 35.9354705, -79.0437656 35.9354717, -79.0437772 35.9354742, -79.0437929 35.9354776, -79.0437878 35.935493, -79.043787 35.9354956, -79.0437863 35.9354977, -79.0437742 35.9355344, -79.0437642 35.9355648, -79.0438097 35.9355748, -79.0438161 35.9355762, -79.0439934 35.9356151, -79.0440005 35.9356167, -79.0440054 35.9356177, -79.0442093 35.9356625, -79.0442112 35.9356629, -79.0442208 35.935665, -79.0443316 35.9356884, -79.0443995 35.9357033, -79.0444271 35.9356249, -79.0444316 35.9356121, -79.0444326 35.9356094, -79.0444522 35.9356138, -79.0445602 35.9356379, -79.0446364 35.9358774), (-79.044481 35.935754, -79.0444466 35.9357482, -79.0444318 35.9357896, -79.0444278 35.9358027, -79.0444263 35.9358078, -79.0444223 35.935821, -79.0444184 35.9358339, -79.0444077 35.9358314, -79.0443733 35.9358235, -79.0443706 35.9358313, -79.0443558 35.9358281, -79.0442931 35.9358146, -79.0441893 35.9357922, -79.0441136 35.9357759, -79.0440799 35.9357686, -79.0439906 35.9357494, -79.0438602 35.9357212, -79.043835 35.9357158, -79.0437312 35.9356934, -79.0437268 35.9356924, -79.0437308 35.9356822, -79.0436865 35.9356921, -79.0436033 35.9357106, -79.0435983 35.9357117, -79.0436029 35.9357253, -79.0436052 35.9357322, -79.0436684 35.9359199, -79.0436739 35.9359363, -79.043677 35.9359454, -79.0436949 35.9359985, -79.0437217 35.9359926, -79.0437377 35.9360403, -79.0437691 35.9360336, -79.043782 35.9360309, -79.0439157 35.9360023, -79.0439246 35.9360004, -79.0439444 35.9359962, -79.0442678 35.9359271, -79.0442761 35.9359254, -79.044465 35.935885, -79.0444693 35.9358841, -79.0444515 35.9358294, -79.044481 35.935754), (-79.0437686 35.9363494, -79.0437385 35.9362576, -79.0437307 35.936234, -79.043707 35.9361616, -79.0436956 35.9361268, -79.0436036 35.9361463, -79.0435929 35.9361485, -79.0435905 35.936141, -79.0434987 35.9361604, -79.0435021 35.9361709, -79.0435114 35.9362, -79.0435168 35.9362169, -79.0436071 35.9361977, -79.0436559 35.9361874, -79.0436739 35.9362435, -79.0436784 35.9362574, -79.0437217 35.9363925, -79.0437289 35.936391, -79.0437417 35.9363883, -79.0437477 35.9363796, -79.0437686 35.9363494)))");
		List<S2CellId> covering = new S2Index().index(10, geom);
		System.out.println(covering.size());
	}

	private S2Polygon makePolygon(String str) {
		List<S2Loop> loops = Lists.newArrayList();

		for (String token : Splitter.on(';').omitEmptyStrings().split(str)) {
			S2Loop loop = makeLoop(token);
			loop.normalize();
			loops.add(loop);
		}

		return new S2Polygon(loops);
	}

	private void parseVertices(String str, List<S2Point> vertices) {
		if (str == null) {
			return;
		}

		for (String token : Splitter.on(',').split(str)) {
			int colon = token.indexOf(':');
			if (colon == -1) {
				throw new IllegalArgumentException("Illegal string:" + token + ". Should look like '35:20'");
			}
			double lat = Double.parseDouble(token.substring(0, colon));
			double lng = Double.parseDouble(token.substring(colon + 1));
			vertices.add(S2LatLng.fromDegrees(lat, lng).toPoint());
		}
	}

	private S2Loop makeLoop(String str) {
		List<S2Point> vertices = Lists.newArrayList();
		parseVertices(str, vertices);
		return new S2Loop(vertices);
	}

}
