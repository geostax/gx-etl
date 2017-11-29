package com.geostax.etl.geoprocessing;

import java.util.Map;
import java.util.Set;

import org.geotools.feature.NameImpl;
import org.geotools.process.Process;
import org.geotools.process.ProcessExecutor;
import org.geotools.process.ProcessFactory;
import org.geotools.process.Processors;
import org.geotools.process.Progress;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.factory.StaticMethodsProcessFactory;
import org.geotools.text.Text;
import org.geotools.util.KVP;
import org.opengis.feature.type.Name;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.OctagonalEnvelope;
import com.vividsolutions.jts.io.WKTReader;

public class ProcessTutorial extends StaticMethodsProcessFactory<ProcessTutorial> {

	public ProcessTutorial() {
		super(Text.text("Tutorial"), "tutorial", ProcessTutorial.class);
	}

	@DescribeProcess(title = "Octagonal Envelope", description = "Get the octagonal envelope of this Geometry.")
	@DescribeResult(description = "octagonal of geom")
	static public Geometry octagonalEnvelope(@DescribeParameter(name = "geom") Geometry geom) {
		return new OctagonalEnvelope(geom).toGeometry(geom.getFactory());
	}

	public static void main(String[] args) throws Exception {
		
		WKTReader wktReader = new WKTReader(new GeometryFactory());
		Geometry geom = wktReader.read("MULTIPOINT (1 1, 5 4, 7 9, 5 5, 2 2)");

		Name name = new NameImpl("geo", "contains");
		Process process = Processors.createProcess(name);

		ProcessExecutor engine = Processors.newProcessExecutor(2);

		// quick map of inputs
		Map<String, Object> input = new KVP("geom", geom);
		Progress working = engine.submit(process, input);

		// you could do other stuff whle working is doing its thing
		if (working.isCancelled()) {
			return;
		}

		Map<String, Object> result = working.get(); // get is BLOCKING
		Geometry octo = (Geometry) result.get("result");

		System.out.println(octo);
		
		
		
        
	}
}
