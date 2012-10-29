package brickhouse.udf.collect;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 *  Cast an Array of objects to an Array of Strings, 
 *    to avoid Hive UDF casting problems
 * @author jeromebanks
 * XXX TODO pass in types to cast to or from 
 * 
 */
public class CastArrayUDF extends GenericUDF {
	private static final Logger LOG = Logger.getLogger(CastArrayUDF.class);
	private ListObjectInspector listInspector;

	
	public List<String> evaluate( List<Object> strArray) {
		List<String> newList = new ArrayList<String>();
		for(Object obj : strArray ) {
      if (obj != null && obj.toString() != null){
			  newList.add( obj.toString() );
      }
		}
		return newList;
	}

	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		List argList = listInspector.getList( arg0[0].get() );
		if(argList != null)
		    return evaluate( argList);
		else 
			 return null;
	}

	@Override
	public String getDisplayString(String[] arg0) {
		return "cast_array()";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0)
			throws UDFArgumentException {
		/// XXX XXX TODO
		///  allow one to specify the type to cast to as an argument
		this.listInspector = (ListObjectInspector) arg0[0];
		LOG.info( " Cast Array input type is " + listInspector + " element = " + listInspector.getListElementObjectInspector());
		
		 
			/// Otherwise, assume we're casting to strings ...
		   ObjectInspector returnType = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		   return returnType;
	}
}
