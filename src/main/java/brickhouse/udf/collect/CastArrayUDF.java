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
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 *  Cast an Array of objects to an Array of Strings, 
 *    to avoid Hive UDF casting problems
 * XXX TODO pass in types to cast to or from 
 * 
 */
public class CastArrayUDF extends GenericUDF {
	private static final Logger LOG = Logger.getLogger(CastArrayUDF.class);
	private ListObjectInspector listInspector;
	///private PrimitiveObjet

	
	public List<String> evaluate( List<Object> strArray) {
		List<String> newList = new ArrayList<String>();
		for(Object obj : strArray ) {
      if (obj != null && obj.toString() != null){
    	  /// XXX TODO 
    	  /// Write Type coersion ...
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

	private static ObjectInspector GetObjectInspectorForTypeName( String typeString) {
		TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeString);
		
		return TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo( typeInfo);
	}
	
	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0)
			throws UDFArgumentException {
		/// XXX XXX TODO
		///  allow one to specify the type to cast to as an argument
		if( arg0[0].getCategory() != Category.LIST ) {
			throw new UDFArgumentException("cast_array() takes a list, and an optional type to cast to.");
		}
		this.listInspector = (ListObjectInspector) arg0[0];
		
		LOG.info( " Cast Array input type is " + listInspector + " element = " + listInspector.getListElementObjectInspector());
	    if( arg0.length > 1) {
	    	if( !( arg0[1] instanceof ConstantObjectInspector)
	    	   || !( arg0[1] instanceof StringObjectInspector) ){
			   throw new UDFArgumentException("cast_array() takes a list, and an optional type to cast to.");
	    	}
	    	ConstantObjectInspector constInsp  = (ConstantObjectInspector) arg0[1];
	    	ObjectInspector newType =  GetObjectInspectorForTypeName( constInsp.getWritableConstantValue().toString() );
		    ObjectInspector returnType = ObjectInspectorFactory.getStandardListObjectInspector(newType);
		   return returnType;
	    }
		
		
		 
			/// Otherwise, assume we're casting to strings ...
		  ObjectInspector returnType = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		  return returnType;
	}
}
