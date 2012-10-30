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


import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.log4j.Logger;


/**
 * Truncate an array, and only return the first N elements
 *
 */
public class TruncateArrayUDF extends GenericUDF {
	private static final Logger LOG = Logger.getLogger( TruncateArrayUDF.class);
	private ListObjectInspector listInspector;
	private IntObjectInspector intInspector;
	
	
    public List<Object> evaluate( List<Object> array, int numVals) {
        List truncatedList = new ArrayList();	
        for(int i=0; i< numVals && i <array.size(); ++i) {
        	truncatedList.add( listInspector.getListElement( array, i ));
        }
        
        return truncatedList;
    }


	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		List argList = listInspector.getList(arg0[0].get() );
		
		int numVals = intInspector.get( arg0[1].get());
		
		return evaluate( argList, numVals);
	}


	@Override
	public String getDisplayString(String[] arg0) {
		return "truncate_array(" + arg0[0] + ", " + arg0[1] + " )";
	}


	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0)
			throws UDFArgumentException {
		ObjectInspector first = ObjectInspectorUtils.getStandardObjectInspector(arg0[0] );
		if(first.getCategory() == Category.LIST) {
			listInspector = (ListObjectInspector) first;
		} else {
			throw new UDFArgumentException(" Exprecting an array as first argument ");
		}
		
		//// 
	    ObjectInspector second = ObjectInspectorUtils.getStandardObjectInspector(arg0[1]);
	    intInspector= (IntObjectInspector) second;
		return first;
	}

}
