package brickhouse.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
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
	  static  String AUTOFLUSH_TAG = "hbase.client.autoflush";
      static  String OVERWRITE_CELL_TAG = "brickhouse.overwrite.cell";

	  private static Map<String, HTable> htableMap = new HashMap<String,HTable>();
	  private static Configuration hbConfig;
	  

	  public static HTable getHTable(Map<String,String> configMap) throws IOException {
		  String tableName = configMap.get(TABLE_NAME_TAG);
		  HTable table = htableMap.get(tableName);
		  if(table == null) {
			  if(hbConfig == null) {
				  Configuration config = new Configuration(true);
				  config.set("hbase.zookeeper.quorum", configMap.get( ZOOKEEPER_QUORUM_TAG));
				  for( Entry<String,String> entry : configMap.entrySet()) {
					  config.set( entry.getKey(), entry.getValue());
				  }
				  hbConfig = HBaseConfiguration.create(config);
			  }

			  table = new HTable(hbConfig, tableName);
			  
			  if(configMap.containsKey(AUTOFLUSH_TAG)) {
				  Boolean flushFlag = Boolean.valueOf( configMap.get(AUTOFLUSH_TAG));
				  table.setAutoFlush(flushFlag);
			  }
			 
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
			LOG.debug( " Uninsp Map = " + uninspMap + " size is " + uninspMap.size());
			Map<String,String> configMap = new HashMap<String,String>();
			for(Object uninspKey : uninspMap.keySet()) {
				Object uninspVal = uninspMap.get( uninspKey);
				String key = ((StringObjectInspector)constMapInsp.getMapKeyObjectInspector()).getPrimitiveJavaObject(uninspKey);
				String val = ((StringObjectInspector)constMapInsp.getMapValueObjectInspector()).getPrimitiveJavaObject(uninspVal);
						
				LOG.debug(" Key " + key + " VAL = " + val );
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
	  
	  
	  /**
	   *  Return a String containing a Byte array 
	   *   corresponding to a primitive object. 
	   *   
	   * @param obj
	   * @param objInsp
	   * @return
	   */
	 public static byte[] getByteArray( Object obj, PrimitiveObjectInspector objInsp) {
			if( obj == null)
				return null;
		    switch( objInsp.getPrimitiveCategory() ) {
		    case STRING : 
		        StringObjectInspector strInspector = (StringObjectInspector) objInsp;
		        return strInspector.getPrimitiveJavaObject(obj).getBytes();
		    case BINARY : 
		        BinaryObjectInspector binInspector = (BinaryObjectInspector) objInsp;
		        return (binInspector.getPrimitiveJavaObject( obj));
		    case LONG :
		    	LongObjectInspector longInspector = (LongObjectInspector) objInsp;
		    	long longVal = longInspector.get( obj);
		    	LOG.debug( " GET BYTE STRING LONG IS " + longVal );

		    	byte[] longBytes = Bytes.toBytes(longVal);
		    	return (longBytes);
		    case DOUBLE :
		    	DoubleObjectInspector doubleInspector = (DoubleObjectInspector) objInsp;
		    	double doubleVal = doubleInspector.get(obj);
		    	byte[] dblBytes = Bytes.toBytes(doubleVal);
		    	return (dblBytes);
		    case INT :
		    	IntObjectInspector intInspector = (IntObjectInspector) objInsp;
		    	int intVal = intInspector.get(obj);
		    	byte[] intBytes = Bytes.toBytes(intVal);
		    	return (intBytes);
		    case FLOAT :
		    	FloatObjectInspector floatInspector = (FloatObjectInspector) objInsp;
		    	float floatVal = floatInspector.get(obj);
		    	byte[] floatBytes = Bytes.toBytes(floatVal);
		    	return (floatBytes);
		    case SHORT :
		    	ShortObjectInspector shortInspector = (ShortObjectInspector) objInsp;
		    	short shortVal = shortInspector.get(obj);
		    	byte[] shortBytes = Bytes.toBytes(shortVal);
		    	return (shortBytes);
		    case BYTE :
		    	ByteObjectInspector byteInspector = (ByteObjectInspector) objInsp;
		    	byte byteVal = byteInspector.get(obj);
		    	byte[] byteBytes = new byte[] { byteVal } ;
		    	return (byteBytes);
		     default :
		    	 LOG.error(" Unknown Primitive Category " + objInsp.getCategory());
		        return null; 
		    }
		}
		

}
