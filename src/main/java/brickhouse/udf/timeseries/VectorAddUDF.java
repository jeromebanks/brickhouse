package brickhouse.udf.timeseries;
/**
 * Copyright 2012 Klout, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/


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
 * Add two vectors together .
 * XXX TODO --- Does this make sense to use HoneyDog , and use commons math library ???
 * 
 */
@Description(
		 name = "vector_add",
		 value = " Add two vectors together."
)
public class VectorAddUDF extends GenericUDF {
	private static final Logger LOG = Logger.getLogger(VectorAddUDF.class);
	private ListObjectInspector vec1Inspector;
	private ListObjectInspector vec2Inspector;
	private StandardListObjectInspector retInspector;

	
	private Double getVectorDouble( ListObjectInspector listInspector, Object obj) throws HiveException {
		Object dblObj = ((PrimitiveObjectInspector)(listInspector.getListElementObjectInspector())).getPrimitiveJavaObject( obj);
		if(dblObj instanceof Number) {
			   Number dblNum = (Number)	dblObj;
			   return dblNum.doubleValue();
		} else {
			   //// Try to coerce it otherwise
				String dblStr = ( dblObj.toString());
				try {
					Double dblCoerce = Double.parseDouble(dblStr);
					return dblCoerce;
				} catch(NumberFormatException formatExc) {
					LOG.info(" Unable to interpret " + dblStr + " as a number");
					throw new HiveException(" Can't interpret " + dblStr + " as a double");
				}
		}
		
	}
	
	
	public List<Double> evaluate( List<Object> vec1, List<Object> vec2) throws HiveException {
		if( vec1.size() != vec2.size() ) {
			throw new HiveException(" Vector Add - vectors need to be the same length.");
		}
		List valList = (List) retInspector.create(0);
		for(int i=0; i< vec1.size(); ++i) {
			
			double val1 = getVectorDouble( vec1Inspector, vec1.get(i));
			double val2 = getVectorDouble( vec2Inspector, vec2.get(i));
			
			valList.add( val1 + val2);
			
		}
		return valList;
	}

	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		List vec1 = vec1Inspector.getList( arg0[0].get() );
		List vec2 = vec2Inspector.getList( arg0[1].get() );
		if(vec1 != null && vec2 != null)
		    return evaluate(vec1,vec2);
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
		this.vec1Inspector = (ListObjectInspector) arg0[0];
		this.vec2Inspector = (ListObjectInspector) arg0[1];
		 
		StandardListObjectInspector returnType = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
		retInspector = returnType;
		
		
		return returnType;
	}
}
