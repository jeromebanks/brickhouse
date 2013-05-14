package brickhouse.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.log4j.Logger;

/**
 *  Common code to access HTables
 *  
 * @author jeromebanks
 *
 */
public class HTableFactory {
	private static final Logger LOG = Logger.getLogger(HTableFactory.class);

	  static  String FAMILY_TAG = "family";
	  static  String QUALIFIER_TAG = "qualifier";
	  static  String TABLE_NAME_TAG = "table_name";
	  static  String ZOOKEEPER_QUORUM_TAG = "hbase.zookeeper.quorum";

	  private static Map<String, HTable> htableMap = new HashMap<String,HTable>();
	  private static Configuration hbConfig;
	  

	  public static HTable getHTable(Map<String,String> configMap) throws IOException {
		  String tableName = configMap.get(TABLE_NAME_TAG);
		  HTable table = htableMap.get(tableName);
		  if(table == null) {
			  if(hbConfig == null) {
				  Configuration config = new Configuration(true);
				  config.set("hbase.zookeeper.quorum", configMap.get( ZOOKEEPER_QUORUM_TAG));
				  hbConfig = HBaseConfiguration.create(config);
			  }

			  table = new HTable(hbConfig, tableName);
			  table.setAutoFlush(false);
			 
			  htableMap.put(tableName, table);
		  }

		  return table;
	  }
	  
	  public static Map<String,String> getConfigFromConstMapInspector(ObjectInspector objInspector) throws UDFArgumentException {
			if( ! ( ObjectInspectorUtils.isConstantObjectInspector(objInspector))
					|| !(objInspector instanceof StandardConstantMapObjectInspector)) {
			   throw new UDFArgumentException("HBase parameters must be a constant map");
			}
			StandardConstantMapObjectInspector constMapInsp = (StandardConstantMapObjectInspector) objInspector;
			Map<?,?> uninspMap  =  constMapInsp.getWritableConstantValue();
			System.out.println( " Uninsp Map = " + uninspMap + " size is " + uninspMap.size());
			Map<String,String> configMap = new HashMap<String,String>();
			for(Object uninspKey : uninspMap.keySet()) {
				Object uninspVal = uninspMap.get( uninspKey);
				String key = ((StringObjectInspector)constMapInsp.getMapKeyObjectInspector()).getPrimitiveJavaObject(uninspKey);
				String val = ((StringObjectInspector)constMapInsp.getMapValueObjectInspector()).getPrimitiveJavaObject(uninspVal);
						
				LOG.info(" Key " + key + " VAL = " + configMap.get(key) );
				System.out.println(" Key " + key + " VAL = " + configMap.get(key) );
				configMap.put( key, val);
			}
			return configMap;
	  }

	 
	 /**
	   * Throws RuntimeException if config is incomplete.
	   * @param configIn
	   */
	  public static void checkConfig(Map<String, String> configIn) {
	    if (!configIn.containsKey(FAMILY_TAG) ||
	        !configIn.containsKey(QUALIFIER_TAG) ||
	        !configIn.containsKey(TABLE_NAME_TAG) ||
	        !configIn.containsKey(ZOOKEEPER_QUORUM_TAG)) {
	      String errorMsg = "Error while doing HBase operation with config " + configIn + " ; Config is missing for: " + FAMILY_TAG + " or " +
	          QUALIFIER_TAG + " or " + TABLE_NAME_TAG + " or " + ZOOKEEPER_QUORUM_TAG;
	      LOG.error(errorMsg);
	      throw new RuntimeException(errorMsg);
	    }
	  }

}
