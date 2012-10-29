package brickhouse.udf.json;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *  Given a JSON String , return a map of key-value pairs 
 * @author jeromebanks
 *
 */

@Description(name="json_map",
value = "_FUNC_(json) - Returns a map of key-value pairs from a JSON string" 
)
public class JsonMapUDF extends GenericUDF {
	private StringObjectInspector stringInspector;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
		if(arguments.length != 1) {
			throw new UDFArgumentException("Usage : json_map( jsonstring) ");
		}
		if(!arguments[0].getCategory().equals( Category.PRIMITIVE)) {
			throw new UDFArgumentException("Usage : json_map( jsonstring) ");
		}
		
		stringInspector = (StringObjectInspector) arguments[0];
		
		ObjectInspector keyInsp = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		ObjectInspector valueInsp = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector; /// XXX Make value type configurable somehow
		
		MapObjectInspector mapInsp = ObjectInspectorFactory.getStandardMapObjectInspector(keyInsp, valueInsp);
		return mapInsp;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		try {
		    String jsonString =  this.stringInspector.getPrimitiveJavaObject(arguments[0].get());
			
		    ObjectMapper om = new ObjectMapper();
		    Object root = om.readValue(jsonString, Object.class);
		    Map<String,Object> rootAsMap = om.readValue(jsonString, Map.class);
		    
		    Map<String,Double> copyMap = new HashMap<String,Double>();
		    for(String key : rootAsMap.keySet() ) {
		    	Object valObj = rootAsMap.get(key);
		    	if(valObj instanceof Double) {
		    		copyMap.put( key,(Double)valObj);
		    	} else if (valObj instanceof Number) {
		    		
		    		Number num =(Number)valObj;
		    		Double newDouble = num.doubleValue();
		    	
		    		copyMap.put( key, newDouble);
		    				
		    	} else {
		    		System.err.println(" Don't know how to interpret " + valObj);
		    	}
		    	
		    }
		    return copyMap;
		} catch (JsonProcessingException e) {
			throw new HiveException(e);
		} catch (IOException e) {
			throw new HiveException(e);
		}
	}

	@Override
	public String getDisplayString(String[] children) {
		return "json_map( " + children[0] + " )";
	}

}
