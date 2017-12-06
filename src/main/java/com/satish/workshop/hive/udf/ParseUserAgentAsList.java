package com.satish.workshop.hive.udf;

import java.util.ArrayList;
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
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.VersionNumber;
import net.sf.uadetector.service.UADetectorServiceFactory;

public class ParseUserAgentAsList extends GenericUDF{

	StringObjectInspector userAgent;
	
	@Override
	public Object evaluate(DeferredObject[] input_args) throws HiveException {
		
		if (input_args == null || input_args.length < 1) {
            throw new HiveException("input_args is EMPTY");
        }
       /* if (input_args[0].get() == null) {
            throw new HiveException("input_args contains null instead of object");
        }*/
        
		Object input_argsObj = userAgent.getPrimitiveJavaObject(input_args[0].get());
		
		if (input_argsObj == null) {
			return null;
		}
		
		String user_agent = null;
		if (input_argsObj instanceof Text) {
			user_agent = ((Text) input_argsObj).toString();
		} else if (input_argsObj instanceof String) {
			user_agent = (String) input_argsObj;
		} else {
			throw new HiveException(
					"Argument is neither a Text nor String, it is a " + input_argsObj.getClass().getCanonicalName());
		}
		
		System.out.println("user_agent = "+user_agent);
		
		UserAgentStringParser parser = UADetectorServiceFactory.getResourceModuleParser();
		ReadableUserAgent agent = parser.parse(user_agent);
		VersionNumber browserVersion = agent.getVersionNumber();
		
		System.out.println("Browser type: " + agent.getType().getName());
        System.out.println("Browser agent: " + agent.getName());        
        System.out.println("Browser version: " + browserVersion.toVersionString());
		
//        UserAgent userAgent = new UserAgent(agent.getType().getName(), agent.getName(), browserVersion.toVersionString());
//        ArrayList<UserAgent> result = new ArrayList<UserAgent>();
//        result.add(userAgent);
        
        Object[] ua = new Object[4];
		ua[0] = new Text(agent.getType().getName());
        ua[1] = new Text(agent.getName());
        ua[2] = new Text(browserVersion.toVersionString());
        
        ArrayList<Object> result = new ArrayList<Object>();
      result.add(ua);
        return result;
		
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
		
		// =================
		// Define the field names for the struct<> and their types
        ArrayList<String> structFieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();

        // fill struct field names
        // type
        structFieldNames.add("type");
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        //family
        structFieldNames.add("agent");
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        // OS name
        structFieldNames.add("version");
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
       
        ObjectInspector structObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames,
                structFieldObjectInspectors);
		// ===================
        StandardListObjectInspector si = ObjectInspectorFactory.getStandardListObjectInspector(structObjectInspector);
        
//        StandardListObjectInspector si = ObjectInspectorFactory.getStandardListObjectInspector(
//        		PrimitiveObjectInspectorFactory.getPrimitiveObjectInspectorFromClass(UserAgent.class));
        return si;
	}

}
