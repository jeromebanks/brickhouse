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
import org.apache.hadoop.hbase.client.Put;
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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.log4j.Logger;

/**
 *  Perform a batch put into HBase,
 *  but as a UDF, given an array of 
 *  keys and values, instead of 
 *   a single aggregating UDAF
 *
 */
@Description(name="hbase_batch_put_array",
value = "_FUNC_(config, key_array, val_array) - Do a batch HBase Put on a table, given an array of keys and values " 
)
public class BatchPutArrayUDF extends GenericUDF {
	private static final Logger LOG = Logger.getLogger( BatchPutArrayUDF.class);
	private ListObjectInspector listKeyInspector;
	private StringObjectInspector keyInspector;
	private ListObjectInspector listValInspector;
	private PrimitiveObjectInspector elemInspector;
	private Map<String,String> configMap;
	
	private HTable table;

	
	///public List<String> evaluate( Map<String,String> config, List<String> keys) {
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		try {
		   if(table == null)
	         table = HTableFactory.getHTable( configMap);
	       
		   Object listKeyObj = arg0[1].get();
		   Object listValObj = arg0[2].get();
	       List<Put> putArr = new ArrayList<Put>();
	       int listLen = listKeyInspector.getListLength( listKeyObj);
	       int listValLen = listValInspector.getListLength( listValObj);
	       if( listLen != listValLen) {
	    	   throw new HiveException(" Array lengths must be the same :: Number of Keys = " + listLen + " ; Number of values = " + listValLen);
	       }
	       for(int i=0; i<listLen; ++i) {
	    	   Object uninspKey = listKeyInspector.getListElement(listKeyObj, i);
	    	   String key = keyInspector.getPrimitiveJavaObject(uninspKey);
	    	   LOG.info( " HB KEY IS " + key );
	    	   Object uninspVal = listValInspector.getListElement( listValObj, i);
	    	   byte[] bytes = HTableFactory.getByteArray( uninspVal, elemInspector);

	    	   Put thePut = new Put(key.getBytes() );
	    	   thePut.add( configMap.get(HTableFactory.FAMILY_TAG).getBytes(), configMap.get(HTableFactory.QUALIFIER_TAG).getBytes(),
	    			   						bytes );
	    	   table.validatePut(thePut);

	           putArr.add( thePut);
	       }
	       table.put( putArr);
	       table.flushCommits();
	      
	       return "Put " + listLen + " records into " + table.getName() + " Family "  + configMap.get( HTableFactory.FAMILY_TAG ) + " ; Qualifier = " + configMap.get( HTableFactory.QUALIFIER_TAG ); 
		} catch(Exception exc ) {
			 LOG.error(" Error while trying HBase PUT ",exc);
			 throw new RuntimeException(exc);
		}
		
		
	}

	@Override
	public String getDisplayString(String[] arg0) {
		return "hbase_batch_put_array( " + arg0[0] + " ) ";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0)
			throws UDFArgumentException {
       if( arg0.length !=3 ) {
    	   throw new UDFArgumentException(" hbase_batch_put_array expects a config map, an array of keys, and an array of values [0]");
       }
       if(arg0[0].getCategory() != Category.MAP)  {
    	   throw new UDFArgumentException(" hbase_batch_put_array expects a config map, an array of keys, and an array of values [1]");
       }
       configMap = HTableFactory.getConfigFromConstMapInspector(arg0[0]);
	   HTableFactory.checkConfig( configMap);
       
       
       if(arg0[1].getCategory() != Category.LIST) {
    	   throw new UDFArgumentException(" hbase_batch_put_array expects a config map, an array of keys, and an array of values [2]");
       }
       listKeyInspector = (ListObjectInspector) arg0[1];
       if(listKeyInspector.getListElementObjectInspector().getCategory() != Category.PRIMITIVE
    		   || ((PrimitiveObjectInspector)(listKeyInspector.getListElementObjectInspector())).getPrimitiveCategory() != PrimitiveCategory.STRING ) {
    	   throw new UDFArgumentException(" hbase_batch_put_array expects a config map, an array of keys, and an array of values [3]");
       }
       keyInspector = (StringObjectInspector) listKeyInspector.getListElementObjectInspector();

       if(arg0[2].getCategory() != Category.LIST) {
    	   throw new UDFArgumentException(" hbase_batch_put_array expects a config map, an array of keys, and an array of values [4]");
       }
       if(listKeyInspector.getListElementObjectInspector().getCategory() != Category.PRIMITIVE) {
    	   throw new UDFArgumentException(" hbase_batch_put_array expects a config map, an array of keys, and an array of values [5]");
       }
       listValInspector = (ListObjectInspector) arg0[2];
       elemInspector = (PrimitiveObjectInspector) listValInspector.getListElementObjectInspector();

       return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	}
	
}
