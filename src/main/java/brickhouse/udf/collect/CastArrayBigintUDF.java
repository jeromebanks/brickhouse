package brickhouse.udf.collect;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.log4j.Logger;

/**
 *  Cast an Array of Strings to an Array of Longs, 
 *    to avoid Hive UDF casting problems
 * @author jeromebanks
 * XXX TODO pass in types to cast to or from 
 * 
 * XXX Don't know how to parameterize, since return type is dynamic 
 * 
 */
public class CastArrayBigintUDF extends GenericUDF {
	private static final Logger LOG = Logger.getLogger(CastArrayBigintUDF.class);
	private ListObjectInspector listInspector;

	
	public List<Long> evaluate( List<Object> strArray) {
		List<Long> newList = new ArrayList<Long>();
		for(Object obj : strArray ) {
			newList.add(Long.parseLong(obj.toString() ));
		}
		return newList;
	}

	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		List argList = listInspector.getList( arg0[0].get() );
		return evaluate( argList);
	}

	@Override
	public String getDisplayString(String[] arg0) {
		return "cast_array_bigint()";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0)
			throws UDFArgumentException {
		/// XXX XXX TODO
		///  allow one to specify the type to cast to as an argument
		this.listInspector = (ListObjectInspector) arg0[0];
		LOG.info( " Cast Bigint Array input type is " + listInspector + " element = " + listInspector.getListElementObjectInspector());
		
		ObjectInspector returnType = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
		return returnType;
	}
}
