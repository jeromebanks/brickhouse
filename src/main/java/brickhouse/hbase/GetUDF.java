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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.log4j.Logger;

/**
 *   Simple UDF for doing single PUT into HBase table ..
 *  Not intended for doing massive reads from HBase,
 *   but only when relatively few rows are being read.  
 *   
 *
 */
@Description(name="hbase_get",
value = 
    "string _FUNC_(config, key, value) - Do a single HBase Get on a table, with a key and an optional type string " +
    " Config must contain zookeeper \n" +
    "quorum, table name, column, and qualifier. Example of usage: \n" +
    "  hbase_get(map('hbase.zookeeper.quorum', 'hb-zoo1,hb-zoo2', \n" +
    "                'table_name', 'metrics', \n" +
    "                'family', 'c', \n" +
    "                'qualifier', 'q'), \n" +
    "            'test.prod.visits.total', \n" +
    "            'bigint') "
)
public class GetUDF extends GenericUDF {
	private static final Logger LOG = Logger.getLogger( BatchGetUDF.class);
	private TypeInfo elementType;
	private StringObjectInspector keyInspector;
	private PrimitiveObjectInspector retInspector;
	private Map<String,String> configMap;
	
	private HTable table;

	public String evaluate( Map<String,String>  config, String key) {
		try {
	       HTable table = HTableFactory.getHTable( config);
	       Get theGet = new Get( key.getBytes());
	       Result res = table.get( theGet);
	       
	       byte[] valBytes = res.getValue(config.get(HTableFactory.FAMILY_TAG).getBytes(),config.get(HTableFactory.QUALIFIER_TAG).getBytes());
	       if( valBytes != null) {
	    	  return new String( valBytes);
	       }
	       return null;
		} catch(Exception exc ) {
			 ///LOG.error(" Error while trying HBase PUT ",exc);
			 throw new RuntimeException(exc);
		}
	}

	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDisplayString(String[] arg0) {
		// TODO Auto-generated method stub
		return null;
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
	       
	       
		return null;
	}
	
}
