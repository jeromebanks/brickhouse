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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/**
 *  Simple UDF for doing single PUT into HBase table.
 *  NOTE: Not intended for doing massive reads from HBase, but only when relatively few rows are being read.
 *
 */
@Description(name="hbase_put",
    value = 
        "string _FUNC_(config, key, value) - Do a HBase Put on a table. " +
        " Config must contain zookeeper \n" +
        "quorum, table name, column, and qualifier. Example of usage: \n" +
        "  hbase_put(map('hbase.zookeeper.quorum', 'hb-zoo1,hb-zoo2', \n" +
        "                'table_name', 'metrics', \n" +
        "                'family', 'c', \n" +
        "                'qualifier', 'q'), \n" +
        "            'test.prod.visits.total', \n" +
        "            '123456') "
)
public class PutUDF extends GenericUDF {
  private static final Logger LOG = Logger.getLogger(PutUDF.class);
  private StringObjectInspector keyInspector;
  private PrimitiveObjectInspector valInspector;
  private Map<String,String> configMap;
	
   private HTable table;




  public String evaluate(Map<String, String> configMap, String key, String value) {
    HTableFactory.checkConfig(configMap);

    try {
      HTable table = HTableFactory.getHTable(configMap);
      Put thePut = new Put(key.getBytes());
      thePut.add(configMap.get(HTableFactory.FAMILY_TAG).getBytes(), configMap.get(HTableFactory.QUALIFIER_TAG).getBytes(), value.getBytes());

      table.put(thePut);
      table.flushCommits();
      return "Put " + key + ":" + value;
    } catch(Exception exc) {
      LOG.error("Error while doing HBase Puts");
      throw new RuntimeException(exc);
    }
  }

  public String evaluate(Map<String, String> configMap, Map<String, String> keyValueMap) {
    HTableFactory.checkConfig(configMap);

    try {
      List<Put> putList = new ArrayList<Put>();
      for (Map.Entry<String, String> keyValue : keyValueMap.entrySet()) {
        Put thePut = new Put(keyValue.getKey().getBytes());
        thePut.add(configMap.get(HTableFactory.FAMILY_TAG).getBytes(),
                   configMap.get(HTableFactory.QUALIFIER_TAG).getBytes(),
                   keyValue.getValue().getBytes());
        putList.add(thePut);
      }

      HTable table = HTableFactory.getHTable(configMap);
      table.put(putList);
      table.flushCommits();
      return "Put " + keyValueMap.toString();
    } catch(Exception exc) {
      LOG.error("Error while doing HBase Puts");
      throw new RuntimeException(exc);
    }
  }

@Override
public Object evaluate(DeferredObject[] arg0) throws HiveException {
	String key = keyInspector.getPrimitiveJavaObject(arg0[1].get() );
	
	try {
	  Object uninspVal = arg0[2].get();
 	  byte[] bytes = HTableFactory.getByteArray( uninspVal, valInspector);
 	  if(table == null) {
       table = HTableFactory.getHTable(configMap);
 	  }
      Put thePut = new Put(key.getBytes());
       thePut.add(configMap.get(HTableFactory.FAMILY_TAG).getBytes(), configMap.get(HTableFactory.QUALIFIER_TAG).getBytes(), bytes);

      table.put(thePut);
      table.flushCommits();
      return "Put " + key + ":" + new String(bytes); /// ?? What to return 
    } catch(Exception exc) {
      return "ERROR while putting key "+ key + "::" + exc.getMessage();
    }
}

@Override
public String getDisplayString(String[] arg0) {
	 return "hbase_put( " + arg0[0] + "," + arg0[1] + "," + arg0[2] + " )";
}

@Override
   public ObjectInspector initialize(ObjectInspector[] arg0)
		throws UDFArgumentException {
    if( arg0.length !=3 ) {
 	   throw new UDFArgumentException(" hbase_put expects a config map, a String key, and a values [0]");
    }
    if(arg0[0].getCategory() != Category.MAP)  {
 	   throw new UDFArgumentException(" hbase_put expects a config map, a String key, and a values [1]");
    }
    configMap = HTableFactory.getConfigFromConstMapInspector(arg0[0]);
	HTableFactory.checkConfig( configMap);
	
    if(arg0[1].getCategory() != Category.PRIMITIVE
    		|| ((PrimitiveObjectInspector)arg0[1]).getPrimitiveCategory() != PrimitiveCategory.STRING )  {
 	   throw new UDFArgumentException(" hbase_put expects a config map, a String key, and a values [2]");
    }
    keyInspector = (StringObjectInspector) arg0[1];
	
    if(arg0[2].getCategory() != Category.PRIMITIVE ) {
 	   throw new UDFArgumentException(" hbase_put expects a config map, a String key, and a values [3]");
    }
    valInspector = (PrimitiveObjectInspector) arg0[2];
    
    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }


}
