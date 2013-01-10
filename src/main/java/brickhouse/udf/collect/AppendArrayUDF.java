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

import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

/**
 *  Append an object to the end of an Array
 *
 * XXX Fix possible ObjectInspector problems
 */
public class AppendArrayUDF extends GenericUDF {
	private ListObjectInspector listInspector;
	private PrimitiveObjectInspector listElemInspector;
	private PrimitiveObjectInspector primInspector;

	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		List objList = listInspector.getList( arg0[0].get());
		Object primObj = primInspector.getPrimitiveJavaObject( arg0[1].get() );
		objList.add( listElemInspector.copyObject( primObj));
		
		return objList;
	}

	@Override
	public String getDisplayString(String[] arg0) {
		return "append_array()";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0)
			throws UDFArgumentException {
		
		if( ((ObjectInspector)arg0[0]).getCategory() != Category.LIST ) {
			throw new UDFArgumentException("append_array expects a list as the first argument");
		}
		if( ((ObjectInspector)arg0[1]).getCategory() != Category.PRIMITIVE ) {
			throw new UDFArgumentException("append_array expects a primitive as the second argument");
		}
		listInspector = (ListObjectInspector) arg0[0];
		primInspector= (StringObjectInspector) arg0[1];
		
		if( listInspector.getListElementObjectInspector().getCategory() != Category.PRIMITIVE
				|| ((PrimitiveObjectInspector)listInspector.getListElementObjectInspector()).getPrimitiveCategory() != primInspector.getPrimitiveCategory() ) {
			throw new UDFArgumentException("append_array expects the list type to match the type of the value being appended");
		}
		listElemInspector = (PrimitiveObjectInspector) listInspector.getListElementObjectInspector();
		
		return listInspector;
	}
}
