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
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;

/**
 *  Simple UDF for doing single PUT into HBase table.
 *  NOTE: Not intended for doing massive reads from HBase, but only when relatively few rows are being read.
 *
 */
@Description(name="hbase_put",
    value = "string _FUNC_(config, key, value) - Do a single HBase Put on a table.  Config must zookeeper quorum, " +
        "table name, and column qualifier. Example of usage: \n" +
        "  hbase_put(map('hbase.zookeeper.quorum', 'hb-zoo1,hb-zoo2', \n" +
        "                'table_name', 'metrics', " +
        "                'family', 'c', " +
        "                'qualifier', 'q'), " +
        "            'test.prod.visits.total', " +
        "            '123456') "
)
public class PutUDF extends UDF {
  private static final Logger LOG = Logger.getLogger(PutUDF.class);

  static private String FAMILY_TAG = "family";
  static private String QUALIFIER_TAG = "qualifier";
  static private String TABLE_NAME_TAG = "table_name";
  static private String ZOOKEEPER_QUORUM_TAG = "hbase.zookeeper.quorum";

  private static Map<String, HTable> htableMap_ = new HashMap<String,HTable>();
  private static Configuration config_ = new Configuration(true);

  public String evaluate(Map<String, String> config, String key, String value) {
    if (!config.containsKey(FAMILY_TAG) ||
        !config.containsKey(QUALIFIER_TAG) ||
        !config.containsKey(TABLE_NAME_TAG) ||
        !config.containsKey(ZOOKEEPER_QUORUM_TAG)) {
      String errorMsg = "Error while doing HBase Puts. Config is missing for: " + FAMILY_TAG + " or " +
      QUALIFIER_TAG + " or " + TABLE_NAME_TAG + " or " + ZOOKEEPER_QUORUM_TAG;
      LOG.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }

    try {
      HTable table = getHTable(config.get(TABLE_NAME_TAG), config.get(ZOOKEEPER_QUORUM_TAG));
      Put thePut = new Put(key.getBytes());
      thePut.add(config.get(FAMILY_TAG).getBytes(), config.get(QUALIFIER_TAG).getBytes(), value.getBytes());

      table.put(thePut);
      return "Put " + key + ":" + value;
    } catch(Exception exc) {
      LOG.error("Error while doing HBase Puts");
      throw new RuntimeException(exc);
    }
  }

  private HTable getHTable(String tableName, String zkQuorum) throws IOException {
    HTable table = htableMap_.get(tableName);
    if(table == null) {
      config_ = new Configuration(true);
      Iterator<Entry<String,String>> iter = config_.iterator();
      while(iter.hasNext()) {
        Entry<String,String> entry =  iter.next();
        LOG.info("BEFORE CONFIG = " + entry.getKey() + " == " + entry.getValue());
      }
      config_.set("hbase.zookeeper.quorum", zkQuorum);
      Configuration hbConfig = HBaseConfiguration.create(config_);
      iter = hbConfig.iterator();
      while(iter.hasNext()) {
        Entry<String,String> entry =  iter.next();
        LOG.info("AFTER CONFIG = " + entry.getKey() + " == " + entry.getValue());
      }
      table = new HTable(hbConfig, tableName);
      htableMap_.put(tableName, table);
    }

    return table;
  }
}
