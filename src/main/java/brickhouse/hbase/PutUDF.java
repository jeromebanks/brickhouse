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

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Simple UDF for doing single PUT into HBase table.
 * NOTE: Not intended for doing massive reads from HBase, but only when relatively few rows are being read.
 */
@Description(name = "hbase_put",
        value = "string _FUNC_(config, map<string, string> key_value) - \n" +
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
public class PutUDF extends UDF {
    private static final Logger LOG = Logger.getLogger(PutUDF.class);


    public String evaluate(Map<String, String> configMap, String key, String value) {
        HTableFactory.checkConfig(configMap);

        try {
            HTable table = HTableFactory.getHTable(configMap);
            Put thePut = new Put(key.getBytes());
            thePut.add(configMap.get(HTableFactory.FAMILY_TAG).getBytes(), configMap.get(HTableFactory.QUALIFIER_TAG).getBytes(), value.getBytes());

            table.put(thePut);
            table.flushCommits();
            return "Put " + key + ":" + value;
        } catch (Exception exc) {
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
        } catch (Exception exc) {
            LOG.error("Error while doing HBase Puts");
            throw new RuntimeException(exc);
        }
    }


}
