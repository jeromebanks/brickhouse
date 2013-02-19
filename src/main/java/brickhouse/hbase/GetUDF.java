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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 *   Simple UDF for doing single PUT into HBase table ..
 *  Not intended for doing massive reads from HBase,
 *   but only when relatively few rows are being read.  
 *   
 *
 */
@Description(name="hbase_get",
value = "_FUNC_(table,key,family) - Do a single HBase Get on a table " 
)
public class GetUDF extends UDF {
	private static Map<String,HTable> htableMap = new HashMap<String,HTable>();
	private static Configuration config = new Configuration(true);

	
	public String evaluate( String tableName, String key) {
		try {
	       HTable table = getHTable( tableName);
	       Get theGet = new Get( key.getBytes());
	       Result res = table.get( theGet);
	       ImmutableBytesWritable bytes=  res.getBytes();
	       return new String( bytes.get());
		} catch(Exception exc ) {
			 ///LOG.error(" Error while trying HBase PUT ",exc);
			 throw new RuntimeException(exc);
		}
		
		
	}
	
	private HTable getHTable(String tableName ) throws IOException {
	   HTable table = htableMap.get( tableName);
	   if(table == null) {
			   config.set("hbase.zookeeper.quorum", "jobs-dev-zoo1,jobs-dev-zoo2,jobs-dev-zoo3");

			  table =   new HTable( HBaseConfiguration.create(config), tableName);
			  htableMap.put( tableName, table);
	   }
	
	   return table;
	}
}
