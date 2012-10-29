package brickhouse.udf.timeseries;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.log4j.Logger;

/**
 * Multiply a vector of numbers times a scalar value
 * 
 */
@Description(
		 name = "vector_mult",
		 value = " Multiply a vector times a scalar"
)
public class VectorMultUDF extends GenericUDF {
	private static final Logger LOG = Logger.getLogger(VectorMultUDF.class);
	private ListObjectInspector listInspector;
	private StandardListObjectInspector retInspector;
	private DoubleObjectInspector dblInspector;

	
	public List<Double> evaluate( List<Object> strArray, Double val) {
		List valList = (List) retInspector.create( strArray.size() );
		for(Object obj : strArray ) {
		
			Object dblObj = ((PrimitiveObjectInspector)(listInspector.getListElementObjectInspector())).getPrimitiveJavaObject( obj);
			if(dblObj instanceof Number) {
			   Number dblNum = (Number)	dblObj;
			   Double newVal= dblNum.doubleValue()* val;
			   valList.add(newVal);
			} else {
			   //// Try to coerce it otherwise
				String dblStr = ( dblObj.toString());
				try {
					Double dblCoerce = Double.parseDouble(dblStr);
			        valList.add(dblCoerce*val);
				} catch(NumberFormatException formatExc) {
					LOG.info(" Unable to interpret " + dblStr + " as a number");
				}
			}
			
		}
		return valList;
	}

	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		List argList = listInspector.getList( arg0[0].get() );
		Double dbl = dblInspector.get( arg0[1].get() );
		if(argList != null)
		    return evaluate( argList, dbl);
		else 
			 return null;
	}

	@Override
	public String getDisplayString(String[] arg0) {
		return "sum_array()";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0)
			throws UDFArgumentException {
		//// XXX TODO Check return types ...
		this.listInspector = (ListObjectInspector) arg0[0];
		 
		StandardListObjectInspector returnType = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
		retInspector = returnType;
		
		dblInspector = (DoubleObjectInspector) arg0[1];
		
		
		return returnType;
	}
}
