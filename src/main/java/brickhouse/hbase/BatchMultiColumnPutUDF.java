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

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *  Perform a batch put into HBase given an hbase key, array of column qualifiers and array of values
 *
 *  This UDF by default does a put that can override initial cell values of hbase records, however, that behaviour can
 *  be changed when the "brickhouse.overwrite.cell" config property is set to "n" in the config map. In such case,
 *  this UDF will perform an hbase GET first to find out if a given record exists in hbase before inserting it.
 *
 *  Usage example:
 *   hbase_batch_put_columns( map("table_name", "xunit_segment",
 *                                "family", "s",
 *                                "hbase.zookeeper.quorum" , "dahdp2jt01.tag-dev.com,dahdp2nn01.tag-dev.com,dahdp2rm01.tag-dev.com",
 *                                "zookeeper.znode.parent", "/hbase-unsecure",
 *                                "brickhouse.overwrite.cell", "n"),  -- hbase config map
 *                            "02:/event/e=friend_request", -- hbase row key
 *                            array("/age", "/device"), -- column qualifier array
 *                            array(1425584347000, 1425584347000)) -- column value array
 *
 */
@Description(name="hbase_batch_put_columns",
value = "_FUNC_(config, key, col_array, val_array) - Do a batch HBase Put on a table, given a key and an array of columns and values "
)
public class BatchMultiColumnPutUDF extends GenericUDF {
    private static final Logger LOG = Logger.getLogger( BatchMultiColumnPutUDF.class);
	private ListObjectInspector listColumnInspector;
    private StringObjectInspector columnInspector;

	private StringObjectInspector keyInspector;
	private ListObjectInspector listValInspector;
	private PrimitiveObjectInspector elemInspector;
	private Map<String,String> configMap;
	
	private HTable table;
    private Boolean overwriteCellValue;
	
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		try {
            if(table == null)
               table = HTableFactory.getHTable( configMap );

            // Default overwriteCellValue to true when config not provided.
            // This allows for new version of column cell value for given row key, col family, col qual
            if(overwriteCellValue == null)
                overwriteCellValue = configMap.containsKey(HTableFactory.OVERWRITE_CELL_TAG)
                        ? toBoolean(configMap.get(HTableFactory.OVERWRITE_CELL_TAG)) : true;

            Object key = arg0[1].get();
            Object listColObj = arg0[2].get();
            Object listValObj = arg0[3].get();

            List<Put> putArr = new ArrayList<Put>();
	        int listColLen = listColumnInspector.getListLength( listColObj );
	        int listValLen = listValInspector.getListLength( listValObj );
	        if( listColLen != listValLen) {
                throw new HiveException(" Array lengths must be the same :: Number of Columbns = " + listColLen
                       + " ; Number of values = " + listValLen);
	        }
            String hbaseKey = keyInspector.getPrimitiveJavaObject(key);
            LOG.info(" HB KEY IS " + hbaseKey);
            byte[] family = configMap.get(HTableFactory.FAMILY_TAG).getBytes();
            for(int i=0; i<listColLen; ++i) {
                Object uninspCol = listColumnInspector.getListElement(listColObj, i);
                byte[] colQualBytes = HTableFactory.getByteArray(uninspCol, columnInspector);
                Object uninspVal = listValInspector.getListElement(listValObj, i);
                byte[] valueBytes = HTableFactory.getByteArray(uninspVal, elemInspector);
                boolean doPut = true;

                if (! overwriteCellValue) {
                    Get get = new Get(hbaseKey.getBytes());
                    get.addFamily(family);
                    get.addColumn(family, colQualBytes);
                    doPut = table.exists(get) ? false : true;
                }

                if(doPut) {
                    Put thePut = new Put(hbaseKey.getBytes());
                    thePut.add(family, colQualBytes, valueBytes);
                    table.validatePut(thePut);
                    putArr.add(thePut);
                }
	        }
            if( putArr.size() > 0) {
                table.put(putArr);
                table.flushCommits();
            }
	      
	        return "Put " + putArr.size() + " columns into " + table.getName() + "Row " + hbaseKey + " Family " + configMap.get( HTableFactory.FAMILY_TAG );
        } catch(Exception exc ) {
            LOG.error(" Error while trying HBase PUT ",exc);
			throw new RuntimeException(exc);
		}
	}

	@Override
    public String getDisplayString(String[] arg0) {
		return "hbase_batch_put_columns( " + arg0[0] + " ) ";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {
        if( arg0.length != 4 ) {
    	    throw new UDFArgumentException(" hbase_batch_put_columns expects a config map, row key, and an array of columns and and array of values [0]");
        }

        // First argument should be a map of hbase configs
        if(arg0[0].getCategory() != Category.MAP)  {
    	    throw new UDFArgumentException(" hbase_batch_put_columns expects a config map, row key, and an array of columns and and array of values [1]");
        }
        configMap = HTableFactory.getConfigFromConstMapInspector(arg0[0]);
	    checkConfig( configMap);

        // Second argument should be an hbase key
        if(! (arg0[1] instanceof StringObjectInspector)) {
            throw new UDFArgumentException(" hbase_batch_put_columns expects a config map, row key, and an array of columns and and array of values [2]");
        }
        keyInspector = (StringObjectInspector) arg0[1];


        // Third argument should be a list of column names (hbase column qualifiers)
        if(arg0[2].getCategory() != Category.LIST) {
    	    throw new UDFArgumentException(" hbase_batch_put_columns expects a config map, row key, and an array of columns and and array of values [3]");
        }
        listColumnInspector = (ListObjectInspector) arg0[2];
        if(listColumnInspector.getListElementObjectInspector().getCategory() != Category.PRIMITIVE
                || ((PrimitiveObjectInspector)(listColumnInspector.getListElementObjectInspector())).getPrimitiveCategory() != PrimitiveCategory.STRING ) {
    	    throw new UDFArgumentException(" hbase_batch_put_columns expects a config map, row key, and an array of columns and and array of values [4]");
        }
        columnInspector = (StringObjectInspector) listColumnInspector.getListElementObjectInspector();

        // Fourth and final argument should be a list of column values
        if(arg0[3].getCategory() != Category.LIST) {
    	    throw new UDFArgumentException(" hbase_batch_put_columns expects a config map, row key, and an array of columns and and array of values [5]");
        }
        listValInspector = (ListObjectInspector) arg0[3];
        if(listValInspector.getListElementObjectInspector().getCategory() != Category.PRIMITIVE) {
    	    throw new UDFArgumentException(" hbase_batch_put_columns expects a config map, row key, and an array of columns and and array of values [6]");
        }
        elemInspector = (PrimitiveObjectInspector) listValInspector.getListElementObjectInspector();

        // return value will be string so create correct output object inspector
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	}

    private Boolean toBoolean(String value) {
        value = value.trim().toLowerCase();
        if(value.equals("y")) {
            return true;
        } else {
            return Boolean.valueOf(value);
        }
    }

    /**
     * Throws RuntimeException if config is incomplete.
     *
     * @param configIn
     */
    private void checkConfig(Map<String, String> configIn) {
        if (!configIn.containsKey(HTableFactory.FAMILY_TAG) ||
                !configIn.containsKey(HTableFactory.TABLE_NAME_TAG) ||
                !configIn.containsKey(HTableFactory.ZOOKEEPER_QUORUM_TAG))
        {
            String errorMsg = "Error while doing HBase operation with config " + configIn + " ; Config is missing for: "
                    + HTableFactory.TABLE_NAME_TAG + " or " + HTableFactory.ZOOKEEPER_QUORUM_TAG;
            LOG.error(errorMsg);
            throw new RuntimeException(errorMsg);
        }
    }
}
