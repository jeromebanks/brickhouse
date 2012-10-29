package brickhouse.udf.collect;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
 * Given a Map, return an Array of structs 
 *  containing key and value
 *  
 * @author jeromebanks
 *
 */

@Description(name="map_key_values",
value = "_FUNC_(map) - Returns a Array of key-value pairs contained in a Map" 
)
public class MapKeyValuesUDF extends GenericUDF {
	private MapObjectInspector moi;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
		if(arguments.length != 1) {
			throw new UDFArgumentException("Usage : map_key_values( map) ");
		}
		if(!arguments[0].getCategory().equals( Category.MAP)) {
			throw new UDFArgumentException("Usage : map_key_values( map) ");
		}
		
		moi= (MapObjectInspector)arguments[0];
		
		//// 
		List<String> structFieldNames =new ArrayList<String>();
		List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
		structFieldNames.add("key");
		structFieldObjectInspectors.add( moi.getMapKeyObjectInspector());
		structFieldNames.add("value");
		structFieldObjectInspectors.add( moi.getMapValueObjectInspector());
		
		ObjectInspector keyOI = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
	    ObjectInspector arrayOI = ObjectInspectorFactory.getStandardListObjectInspector(keyOI);
		
	    return arrayOI;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		Map<?, ?> map= moi.getMap( arguments[0].get() );


		List<List> array = new ArrayList<List>();
		for( Object key : map.keySet() ) {
			List kv = new ArrayList();
			kv.add( key);
			kv.add( map.get( key));
			array.add( kv);


		}
		return array;
	}

	@Override
	public String getDisplayString(String[] children) {
		return "map_key_values( " + children[0] + " )";
	}

}
