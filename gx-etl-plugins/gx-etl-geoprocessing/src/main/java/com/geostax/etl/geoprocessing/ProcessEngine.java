package com.geostax.etl.geoprocessing;

import java.util.Set;

import org.geotools.feature.NameImpl;
import org.geotools.process.Process;
import org.geotools.process.ProcessFactory;
import org.geotools.process.Processors;

import com.vividsolutions.jts.geom.Geometry;

public class ProcessEngine {

	public ProcessEngine() {
		// TODO Auto-generated constructor stub
	}
	
	public static void main(String[] args) {
		Set<ProcessFactory> factories = Processors.getProcessFactories();
        for (ProcessFactory pf : factories) {
    		System.out.println(pf.getNames());
        }
	}
}
