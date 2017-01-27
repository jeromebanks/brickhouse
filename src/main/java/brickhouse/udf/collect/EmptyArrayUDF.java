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


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 *
 *  Create an empty array of a specific type,
 *    as opposed to the 'array()' UDF, which would only return
 *    an empty array of type STRING.
 *
 *    This might be used along with COALESCE, to translate NULL to an empty array,
 *      to be combined with other values...
 *
 */
public class EmptyArrayUDF extends GenericUDF {
	private static final Logger LOG = Logger.getLogger(EmptyArrayUDF.class);
	private StandardListObjectInspector listInspector;

	
	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		return listInspector.create(0);
	}

	@Override
	public String getDisplayString(String[] arg0) {
	    return "empty_array( '" + arg0[0] + "' )";
	}

	private ObjectInspector GetObjectInspectorForTypeName( String typeString) {
		TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeString);
		LOG.info( "Type for " + typeString + " is " + typeInfo);
		
		return TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo( typeInfo);
	}
	
	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0)
			throws UDFArgumentException {
		if( arg0.length != 1
			|| !(arg0[0] instanceof ConstantObjectInspector)
			|| !(arg0[0] instanceof StringObjectInspector) ){
				throw new UDFArgumentException("empty_array() takes a hive type as a single argument.");
		}
		ConstantObjectInspector constInsp  = (ConstantObjectInspector) arg0[0];
		String  arrayType =  constInsp.getWritableConstantValue().toString();
		ObjectInspector elemInspector = GetObjectInspectorForTypeName( arrayType);
		listInspector = ObjectInspectorFactory.getStandardListObjectInspector(elemInspector);
        LOG.info( " Cast Array input type is " + listInspector + " element = " + listInspector.getListElementObjectInspector());
		return listInspector;

	}
}
