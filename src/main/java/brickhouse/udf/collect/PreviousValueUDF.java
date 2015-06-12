package brickhouse.udf.collect;
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

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.log4j.Logger;


/**
 *  PreviousValueUDF returns the value which was passed in the 
 *    previous row of the queru, for some grouping
 *    
 *    This may be useful for checking to see if a value
 *     changes , when sorted a certain way, or 
 *      doing certain windowing like functions i.e
 *      
 *      
 *     SELECT user_id, month, amt, amt + previous_value( user_id,  amt ) AS running_sum
 *      FROM 
 *      payments
 *      DISTRIBUTE BY user_id
 *      SORT BY user_id, month
 *      
 *      
 */
@Description(name="previous_value",
value = "_FUNC_(grouping,val) - value for previous row for specified grouping " 
)
public class PreviousValueUDF extends GenericUDF {
	private static final Logger LOG = Logger.getLogger( PreviousValueUDF.class );
	private PrimitiveObjectInspector valInspector;
	private StringObjectInspector groupingInspector;
	private Object prevValue = null;
	private String prevGrouping;
	

	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {

		String grouping = groupingInspector.getPrimitiveJavaObject(arg0[0].get() );
		Object uninsp = arg0[1].get();
		Object val = valInspector.getPrimitiveJavaObject( uninsp);
		
		
		if( !grouping.equals( prevGrouping)) {
			 LOG.info(" New Grouping " + grouping + " is Not equal to PrevGrouping " + prevGrouping);
             prevValue = val;
             prevGrouping = grouping;
             return null;
		}
		LOG.info( " PREV VAL OS " + prevValue +  " vAL IS " + val );
	    Object retObj = prevValue;	
        prevValue = val;
        return retObj;
	}

	@Override
	public String getDisplayString(String[] arg0) {
		return "previous_value( " + arg0[0] + " , " +  arg0[1]  + " )";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0)
			throws UDFArgumentException {
		if( arg0.length != 2) {
			throw new UDFArgumentException(" previous_value takes two arguments; a grouping and a value ");
		}
		if( arg0[0].getCategory() != Category.PRIMITIVE
				|| ((PrimitiveObjectInspector)arg0[0]).getPrimitiveCategory() != PrimitiveCategory.STRING) {
			throw new UDFArgumentException("previous_value grouping must be a string.");
		}
		if( arg0[1].getCategory() != Category.PRIMITIVE) {
			throw new UDFArgumentException("previous_value value must be a primitive ");
		}
		groupingInspector = (StringObjectInspector) arg0[0];
		valInspector = (PrimitiveObjectInspector) arg0[1];
		
		ObjectInspector stdInspector =  ObjectInspectorUtils.getStandardObjectInspector(valInspector, ObjectInspectorCopyOption.JAVA);
		
		LOG.info(" GROUP INSPECTOR = " + arg0[0]);
		LOG.info(" VAL INSPECTOR = " + arg0[1]);
		LOG.info(" STD VAL INSPECTOR = " + stdInspector);
		return stdInspector;
	}

}
