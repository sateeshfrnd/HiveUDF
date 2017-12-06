package com.satish.workshop.hive.udf;

/**
 * Hello world!
 *
 */
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.VersionNumber;
import net.sf.uadetector.service.UADetectorServiceFactory;

public class ParseUserAgentAsMap extends GenericUDF{

	StringObjectInspector userAgent;
	
	@Override
	public Object evaluate(DeferredObject[] input_args) throws HiveException {
		
		String user_agent = userAgent.getPrimitiveJavaObject(input_args[0].get());
		
		if (user_agent == null) {
			return null;
		}
		
		System.out.println("user_agent = "+user_agent);
		
		UserAgentStringParser parser = UADetectorServiceFactory.getResourceModuleParser();
		ReadableUserAgent agent = parser.parse(user_agent);
		VersionNumber browserVersion = agent.getVersionNumber();
		
		Map<String, String> map = new TreeMap<String, String>();
		System.out.println("Browser type: " + agent.getType().getName());
        System.out.println("Browser agent: " + agent.getName());        
        System.out.println("Browser version: " + browserVersion.toVersionString());
        
		map.put("browser_name", agent.getType().getName());
		map.put("browser_agent", agent.getName());
		map.put("version", browserVersion.toVersionString());
		return map;
	}

	@Override
	public String getDisplayString(String[] arg0) {
		return "Parse User Agent";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] input_args) throws UDFArgumentException {
		
		if(input_args.length != 1) {
			throw new UDFArgumentLengthException("Error : Only takes onw parameter");
		} else if(!(input_args[0] instanceof StringObjectInspector)) {
			throw new UDFArgumentException("Error : Allows only String type");			
		}
		
		this.userAgent = (StringObjectInspector) input_args[0];
		
		return ObjectInspectorFactory.getStandardMapObjectInspector(
				PrimitiveObjectInspectorFactory.javaStringObjectInspector, 
				PrimitiveObjectInspectorFactory.javaStringObjectInspector);
	}

}
