package brickhouse.hbase;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.log4j.Logger;

/**
 *   Simple UDF for doing batch GET's from an HBase table ..
 *  Not intended for doing massive reads from HBase,
 *   but only when relatively few rows are being read.  
 *   
 *
 */
@Description(name="hbase_batch_get",
value = "_FUNC_(table,key,family) - Do a single HBase Get on a table " 
)
public class BatchGetUDF extends GenericUDF {
	private static final Logger LOG = Logger.getLogger( BatchGetUDF.class);
	private TypeInfo elementType;
	private ListObjectInspector listInspector;
	private StandardListObjectInspector retInspector;
	private StringObjectInspector keyInspector;
	private PrimitiveObjectInspector elemInspector;
	private Map<String,String> configMap;
	
	private HTable table;

	
	///public List<String> evaluate( Map<String,String> config, List<String> keys) {
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		try {
		   if(table == null)
	         table = HTableFactory.getHTable( configMap);
	       
		   Object listObj = arg0[1].get();
	       List<Get> getArr = new ArrayList<Get>();
	       int listLen = listInspector.getListLength( listObj);
	       LOG.info(" List Len = " + listLen + " List OBject " + listObj);
	       for(int i=0; i<listLen; ++i) {
	    	   Object uninspKey = listInspector.getListElement(listObj, i);
	    	   String key = keyInspector.getPrimitiveJavaObject(uninspKey);
	    	   LOG.debug(" Adding Key " + key);
	           Get theGet = new Get( key.getBytes());
	           getArr.add( theGet);
	       }
	       Result[] results = table.get( getArr); //// Return multiple at once ????
	       LOG.debug(" Retrieved " + results.length + " results from HBase  " );
	       
	       ArrayList resultList = new ArrayList();
	       for(int i=0;i<results.length; ++i) {
	          Result res = results[i];
	          
	          byte[] valBytes = res.getValue( configMap.get(HTableFactory.FAMILY_TAG).getBytes(), configMap.get(HTableFactory.QUALIFIER_TAG).getBytes());

	          if( valBytes != null) {
	               Object val = getObjectFromBytes( valBytes, elemInspector);
	        	  resultList.add(  val);
	          } else {
	        	  resultList.add( null);
	          }
	       }
	      
	       return resultList;
		} catch(Exception exc ) {
			 LOG.error(" Error while trying HBase PUT ",exc);
			 throw new RuntimeException(exc);
		}
		
		
	}
	
	
	private Object getObjectFromBytes( byte[] bytes, PrimitiveObjectInspector objInsp) {
		if( bytes == null)
			return null;
	    switch( objInsp.getPrimitiveCategory() ) {
	    case STRING : 
	    	return new String( bytes);
	    case BINARY : 
	    	return bytes;
	    case LONG :
	    	long longVal = Bytes.toLong(bytes);

	    	return longVal;
	    case DOUBLE :
	    	double dblVal = Bytes.toDouble(bytes);
	    	return dblVal;
	    case INT :
	    	int intVal = Bytes.toInt( bytes);
	    	return intVal;
	    case FLOAT :
	    	float floatVal = Bytes.toFloat(bytes);
	    	return floatVal;
	    case SHORT :
	    	short shortVal = Bytes.toShort(bytes);
	    	return shortVal;
	    case BYTE :
	    	return bytes[0];
	     default :
	    	 LOG.error(" Unknown Primitive Category " + objInsp.getCategory());
	        return null; 
	    }
	}


	@Override
	public String getDisplayString(String[] arg0) {
		return "hbase_batch_get( " + arg0[0] + " ) ";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0)
			throws UDFArgumentException {
       if( arg0.length != 2 && arg0.length !=3 ) {
    	   throw new UDFArgumentException(" hbase_batch_get expects a config map, an array of keys, and an optional type signature [0]");
       }
       if(arg0[0].getCategory() != Category.MAP)  {
    	   throw new UDFArgumentException(" hbase_batch_get expects a config map, an array of keys, and an optional type signature [1]");
       }
       configMap = HTableFactory.getConfigFromConstMapInspector(arg0[0]);
	   HTableFactory.checkConfig( configMap);
       
       
       if(arg0[1].getCategory() != Category.LIST) {
    	   throw new UDFArgumentException(" hbase_batch_get expects  a config map,an array of keys, and an optional type signature [2]");
       }
       listInspector = (ListObjectInspector) arg0[1];
       if(listInspector.getListElementObjectInspector().getCategory() != Category.PRIMITIVE
    		   || ((PrimitiveObjectInspector)(listInspector.getListElementObjectInspector())).getPrimitiveCategory() != PrimitiveCategory.STRING ) {
    	   throw new UDFArgumentException(" hbase_batch_get expects an array of keys, and an optional type signature");
       }
       keyInspector = (StringObjectInspector) listInspector.getListElementObjectInspector();
       if(arg0.length == 3) {
          if(!(ObjectInspectorUtils.isConstantObjectInspector(arg0[2]))) {
    	    throw new UDFArgumentException(" hbase_batch_get expects  a config map,an array of keys, and an optional type signature [3]");
          }
   	      ConstantObjectInspector constInsp = (ConstantObjectInspector) arg0[2];
    	  String typeName = constInsp.getWritableConstantValue().toString(); 
    	   
    	   elementType = TypeInfoUtils.getTypeInfoFromTypeString(typeName);
       } else {
    	   elementType = TypeInfoUtils.getTypeInfoFromTypeString("string");
       }
       elemInspector = (PrimitiveObjectInspector) TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(elementType);

       retInspector = ObjectInspectorFactory.getStandardListObjectInspector(elemInspector);

       return retInspector;
	}
	
}
