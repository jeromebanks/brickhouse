package brickhouse.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;


import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

/**
 *   Simple UDF for doing single PUT into HBase table ..
 *  Not intended for doing massive reads from HBase,
 *   but only when relatively few rows are being read.  
 *   
 * @author jeromebanks
 *
 */
@Description(name="hbase_put",
value = "_FUNC_(table,key,family) - Do a single HBase Put on a table " 
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
