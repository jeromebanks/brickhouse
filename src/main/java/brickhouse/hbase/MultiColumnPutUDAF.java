package brickhouse.hbase;
/**
 * Copyright 2012 Klout, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in c¬ompliance with the License.
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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Insert into HBase by doing bulk puts on multiple columns and values from an aggregate function call.
 * hbase_multicolumn_put function expects following arguments in the below order:
 *   1. configuration map
 *   2. hbase key value
 *   3. Array of hbase column qualifiers
 *   4. Array of values
 *
 * Example below:
 *
 *   select hbase_multicolumn_put( map("table_name", "mytable",
 *                                     "family", "m",
 *                                     "hbase.zookeeper.quorum", "localhost:2181",
 *                                     "zookeeper.znode.parent", "/",
 *                                     "batch_size", "25000",
 *                                     "write_buffer_size_mb", "12",
 *                                     "disable_auto_flush", "true" ),
 *                                 "abc:20150422",
 *                                 array("colqual1', "colqual2"),
 *                                 array(123456, 3445566) )
 *
 * This function also supports various value types (longs, ints, string, doubles) to be passed into the array as long
 * as they are casted into Strings and the hbase column qualifier specifies how to insert the value into hbase.
 * The example below, inserts both long and double values into hbase column qualifiers "qual1:l" and "qual2:d":
 *
 *   select hbase_multicolumn_put( map("table_name", "mytable",
 *                                     "family", "m",
 *                                     "hbase.zookeeper.quorum", "localhost:2181",
 *                                     "zookeeper.znode.parent", "/",
 *                                     "batch_size", "25000",
 *                                     "write_buffer_size_mb", "12",
 *                                     "disable_auto_flush", "true" ),
 *                                 "abc:20150422",
 *                                 array("qual1:l', "qual2:d"),
 *                                 array(cast(123456 as string), cast(3445566.879 as string)) )
 */

@Description(name = "hbase_multicolumn_put",
        value = "_FUNC_(config_map, key, array(column_qualifiers) array(values) ) - Perform batch HBase updates of a table "
)
public class MultiColumnPutUDAF extends AbstractGenericUDAFResolver {
    private static final Logger LOG = Logger.getLogger(MultiColumnPutUDAF.class);

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        for (int i = 0; i < parameters.length; ++i) {
            LOG.info(" BATCH PUT PARAMETERS : " + i + " -- " + parameters[i].getTypeName() + " cat = " + parameters[i].getCategory());
            System.out.println(" BATCH PUT PARAMETERS : " + i + " -- " + parameters[i].getTypeName() + " cat = " + parameters[i].getCategory());
        }

        return new MultiColumnPutUDAFEvaluator();
    }

    public static class MultiColumnPutUDAFEvaluator extends GenericUDAFEvaluator {
        private Reporter reporter;

        private Reporter getReporter() throws HiveException {
            try {
                if (reporter == null) {
                    Class clazz = Class.forName("org.apache.hadoop.hive.ql.exec.MapredContext");
                    Method staticGetMethod = clazz.getMethod("get");
                    Object mapredObj = staticGetMethod.invoke(null);
                    Class mapredClazz = mapredObj.getClass();
                    Method getReporter = mapredClazz.getMethod("getReporter");
                    Object reporterObj = getReporter.invoke(mapredObj);

                    reporter = (Reporter) reporterObj;
                }

                return reporter;
            } catch(Exception e) {
                throw new HiveException("Error while accessing Hadoop Counters", e);
            }
        }

        public class PutBuffer implements AggregationBuffer {
            public List<Put> putList;

            public PutBuffer() {
            }

            public void reset() {
                putList = new ArrayList<Put>();
            }

            public void addColumnsAndValues(byte[] key, List<byte[]> colQuals, List<byte[]> values) throws HiveException {
                Put thePut = new Put(key);

                //Disable WAL writes when specified in config map
                if (disableWAL) thePut.setDurability(Durability.SKIP_WAL);
                // Loop through columns and values and add them to single put
                byte[] family = getFamily();
                for(int i=0; i<colQuals.size(); i++) {
                    thePut.add(family, colQuals.get(i), values.get(i));
                }

                putList.add(thePut);
            }
        }


        private byte[] getFamily() {
            String famStr = configMap.get(HTableFactory.FAMILY_TAG);
            return famStr.getBytes();
        }


        private int batchSize = 10000;
        private int numPutRecords = 0;
        private boolean disableWAL = false;
        private boolean disableAutoFlush = false;
        private int writeBufferSizeBytes = 0;

        public static final String BATCH_SIZE_TAG = "batch_size";
        public static final String DISABLE_WAL = "disable_wal";
        public static final String DISABLE_AUTO_FLUSH = "disable_auto_flush";
        public static final String WRITE_BUFFER_SIZE_MB = "write_buffer_size_mb";

        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
        private PrimitiveObjectInspector inputKeyOI;
        private StringObjectInspector columnInspector;
        private PrimitiveObjectInspector valueInspector;
        private ListObjectInspector listColumnQualInspector;
        private ListObjectInspector listColumnValInspector;

        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
        // of objs)
        private StandardListObjectInspector listKVOI;
        private Map<String, String> configMap;

        private HTable table;


        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            // init output object inspectors
            ///  input will be key, list of column qualifiers, and a list of values
            LOG.info(" Init mode = " + m);
            System.out.println(" Init mode = " + m);
            System.out.println(" parameters =  = " + parameters + " Length = " + parameters.length);
            configMap = new HashMap<String, String>();
            for (int k = 0; k < parameters.length; ++k) {
                LOG.info("Param " + k + " is " + parameters[k]);
                System.out.println("Param " + k + " is " + parameters[k]);
            }

            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                configMap = HTableFactory.getConfigFromConstMapInspector(parameters[0]);
                checkConfig(configMap);

                inputKeyOI = (PrimitiveObjectInspector) parameters[1];
                listColumnQualInspector = (ListObjectInspector) parameters[2];
                listColumnValInspector = (ListObjectInspector) parameters[3];

                columnInspector = (StringObjectInspector) listColumnQualInspector.getListElementObjectInspector();
                valueInspector = (PrimitiveObjectInspector) listColumnValInspector.getListElementObjectInspector();

                try {
                    LOG.info(" Initializing HTable ");
                    table = HTableFactory.getHTable(configMap);

                    if (configMap.containsKey(BATCH_SIZE_TAG)) {
                        batchSize = Integer.parseInt(configMap.get(BATCH_SIZE_TAG));
                    }

                    if (configMap.containsKey(DISABLE_AUTO_FLUSH)) {
                        LOG.info("Disabling auto flush on hbase puts");
                        disableAutoFlush = Boolean.valueOf(configMap.get(DISABLE_AUTO_FLUSH));
                    }

                    if (configMap.containsKey(DISABLE_WAL)) {
                        LOG.info("Disabling WAL writes on hbase puts");
                        disableWAL = Boolean.valueOf(configMap.get(DISABLE_WAL));
                    }

                    if (configMap.containsKey(WRITE_BUFFER_SIZE_MB)) {
                        LOG.info("Setting hbase write buffer size to: " + writeBufferSizeBytes);
                        writeBufferSizeBytes = Integer.parseInt(configMap.get(WRITE_BUFFER_SIZE_MB)) * 1024 * 1024;
                    }
                } catch (IOException e) {
                    throw new HiveException(e);
                }
            } else {
                listKVOI = (StandardListObjectInspector) parameters[0];
            }

            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                return ObjectInspectorFactory
                        .getStandardListObjectInspector(
                                ObjectInspectorFactory.getStandardListObjectInspector(
                                        PrimitiveObjectInspectorFactory.javaStringObjectInspector)
                        );
            } else {
                /// Otherwise return a message
                return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            PutBuffer buff = new PutBuffer();
            reset(buff);
            return buff;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            byte[] key = HTableFactory.getByteArray( parameters[1], inputKeyOI);
            Object listColObj = parameters[2];
            Object listValObj = parameters[3];
            int listColLen = listColumnQualInspector.getListLength( listColObj );
            int listValLen = listColumnValInspector.getListLength( listValObj );

            if( listColLen != listValLen) {
                throw new HiveException(" Array lengths must be the same :: Number of Columns = " + listColLen
                        + " ; Number of values = " + listValLen + " for key: " + Bytes.toString(key));
            }

            List<byte[]> columns = new ArrayList<byte[]>(listColLen);
            List<byte[]> values = new ArrayList<byte[]>(listValLen);

            //loop through columns and a
            for(int i=0; i<listColLen; i++) {
                Object uninspCol = listColumnQualInspector.getListElement(listColObj, i);
                byte[] colQualBytes = HTableFactory.getByteArray(uninspCol, columnInspector);
                Object uninspVal = listColumnValInspector.getListElement(listValObj, i);
                byte[] valueBytes = null;

                // When column values are String, it is possible that they were casted as such and that the real
                // columnType is contained in the column qualifier. For example "cm_s0:d" where :d means double
                if(valueInspector.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
                    StringObjectInspector soi = (StringObjectInspector) valueInspector;
                    String columnQual = soi.getPrimitiveJavaObject(uninspCol);
                    String value = soi.getPrimitiveJavaObject(uninspVal);

                    if(columnQual.contains(":")) {
                        String[] columnAndType = columnQual.split(":");
                        ColumnType type = ColumnType.fromColumnType(columnAndType[1].toLowerCase());
                        switch (type) {
                            case DOUBLE:
                                valueBytes = Bytes.toBytes(Double.parseDouble(value));
                                break;
                            case INT:
                                valueBytes = Bytes.toBytes(Integer.parseInt(value));
                                break;
                            case LONG:
                                valueBytes = Bytes.toBytes(Long.parseLong(value));
                                break;
                            default:
                                valueBytes = value.getBytes();
                                break;
                        }
                    } else {
                        // By default values are long when column qualifier does not have columnType info
                        valueBytes = Bytes.toBytes(Long.parseLong(value));
                    }
                } else {
                    valueBytes = HTableFactory.getByteArray(uninspVal, valueInspector);
                }

                //String columnQualifier = columnInspector.getPrimitiveJavaObject(uninspCol);
                //String value = valueInspector.getPrimitiveJavaObject(uninspVal).toString();
                //LOG.info("key: " + Bytes.toString(key) + " column: " + columnQualifier + " value: " + value);

                if (key != null) {
                    if( valueBytes != null && colQualBytes != null) {
                        columns.add(colQualBytes);
                        values.add(valueBytes);
                    }
                } else {
                    getReporter().getCounter(MultiColumnPutUDAFCounter.NULL_KEY_INSERT_FAILURE).increment(1);
                }
            }

            PutBuffer kvBuff = (PutBuffer) agg;
            if (kvBuff.putList.size() >= batchSize) {
                batchUpdate(kvBuff, false);
            }

            if(values.size() > 0) {
                kvBuff.addColumnsAndValues(key, columns, values);
            } else {
                getReporter().getCounter(MultiColumnPutUDAFCounter.NULL_VALUES_INSERT_FAILURE).increment(1);
            }
        }


        protected void batchUpdate(PutBuffer kvBuff, boolean flushCommits) throws HiveException {
            try {

                HTable htable = HTableFactory.getHTable(configMap);
                // Disable auto flush when specified so in the config map
                if (disableAutoFlush)
                    htable.setAutoFlushTo(false);

                // Overwrite the write buffer size when config map specifies to do so
                if (writeBufferSizeBytes > 0)
                    htable.setWriteBufferSize(writeBufferSizeBytes);

                htable.put(kvBuff.putList);
                if (flushCommits)
                    htable.flushCommits();
                numPutRecords += kvBuff.putList.size();
                if (kvBuff.putList.size() > 0) {
                    LOG.info(" Doing Batch Put " + kvBuff.putList.size() + " records; Total put records = "
                            + numPutRecords + " ; Start = " + (new String(kvBuff.putList.get(0).getRow()))
                            + " ; End = " + (new String(kvBuff.putList.get(kvBuff.putList.size() - 1).getRow())));
                } else {
                    LOG.info(" Doing Batch Put with ZERO 0 records");
                }
                getReporter().getCounter(MultiColumnPutUDAFCounter.NUMBER_OF_SUCCESSFUL_PUTS).increment(kvBuff.putList.size());
                getReporter().getCounter(MultiColumnPutUDAFCounter.NUMBER_OF_BATCH_OPERATIONS).increment(1);
                kvBuff.putList.clear();
            } catch (IOException e) {
                throw new HiveException(e);
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            LOG.info("!!!! We are in merge !!!!");
            PutBuffer myagg = (PutBuffer) agg;
            List<Object> partialResult = (List<Object>) this.listKVOI.getList(partial);
            ListObjectInspector subListOI = (ListObjectInspector) listKVOI.getListElementObjectInspector();

            List first = subListOI.getList(partialResult.get(0));
            String tableName = ((StringObjectInspector) (subListOI.getListElementObjectInspector())).getPrimitiveJavaObject(first.get(0));
            configMap.put(HTableFactory.TABLE_NAME_TAG, tableName);
            String zookeeper = ((StringObjectInspector) (subListOI.getListElementObjectInspector())).getPrimitiveJavaObject(first.get(1));
            configMap.put(HTableFactory.ZOOKEEPER_QUORUM_TAG, zookeeper);
            String family = ((StringObjectInspector) (subListOI.getListElementObjectInspector())).getPrimitiveJavaObject(first.get(2));
            configMap.put(HTableFactory.FAMILY_TAG, family);
            // Include arbitrary configurations, by adding strings of the form k=v
            for (int j = 3; j < first.size(); ++j) {
                String kvStr = ((StringObjectInspector) (subListOI.getListElementObjectInspector())).getPrimitiveJavaObject(first.get(j));
                String[] kvArr = kvStr.split("=");
                if (kvArr.length == 2) {
                    configMap.put(kvArr[0], kvArr[1]);
                }
            }

            StringObjectInspector strInspector = (StringObjectInspector) subListOI.getListElementObjectInspector();
            for (int i = 1; i < partialResult.size(); ++i) {
                List kvList = subListOI.getList(partialResult.get(i));
                String key = ((StringObjectInspector) (subListOI.getListElementObjectInspector())).getPrimitiveJavaObject(kvList.get(0));


                List uninspColumns = subListOI.getList(kvList.get(1));
                List<byte[]> columns = new ArrayList<byte[]>(uninspColumns.size());
                List uninspValues = subListOI.getList(kvList.get(2));
                List<byte[]> values = new ArrayList<byte[]>(uninspValues.size());

                //This should never happen, but if it does just kill the job!
                if(uninspColumns.size() != uninspValues.size()) {
                    throw new HiveException(" Merge array lengths must be the same :: Number of Columns = " + uninspColumns.size()
                            + " ; Number of values = " + uninspValues.size() + " for key: " + key);
                }

                for(int j=0; i<uninspColumns.size(); j++) {
                    String column = strInspector.getPrimitiveJavaObject(uninspColumns.get(j));
                    String value = strInspector.getPrimitiveJavaObject(uninspValues.get(j));
                    columns.add(column.getBytes());
                    values.add(value.getBytes());
                }

                myagg.addColumnsAndValues(key.getBytes(), columns, values);
            }

            if (myagg.putList.size() >= batchSize) {
                batchUpdate(myagg, false);
            }
        }

        @Override
        public void reset(AggregationBuffer buff) throws HiveException {
            PutBuffer putBuffer = (PutBuffer) buff;
            putBuffer.reset();
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            PutBuffer myagg = (PutBuffer) agg;
            batchUpdate(myagg, true);
            return "Finished Batch updates ; Num Puts = " + numPutRecords;

        }


        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            PutBuffer myagg = (PutBuffer) agg;

            ArrayList<List<String>> ret = new ArrayList<List<String>>();
            ArrayList tname = new ArrayList<String>();
            tname.add(configMap.get(HTableFactory.TABLE_NAME_TAG));
            tname.add(configMap.get(HTableFactory.ZOOKEEPER_QUORUM_TAG));
            tname.add(configMap.get(HTableFactory.FAMILY_TAG));

            for (Entry<String, String> entry : configMap.entrySet()) {
                if (!entry.getKey().equals(HTableFactory.TABLE_NAME_TAG)
                        && !entry.getKey().equals(HTableFactory.ZOOKEEPER_QUORUM_TAG)
                        && !entry.getKey().equals(HTableFactory.FAMILY_TAG)) {

                    tname.add(entry.getKey() + "=" + entry.getValue());
                }
            }
            ret.add(tname);

            for (Put thePut : myagg.putList) {
                ArrayList<String> kvList = new ArrayList<String>();
                kvList.add(new String(thePut.getRow()));
                Map<byte[], List<KeyValue>> familyMap = thePut.getFamilyMap();
                for (List<KeyValue> innerList : familyMap.values()) {
                    for (KeyValue kv : innerList) {
                        kvList.add(new String(kv.getValue()));
                    }
                }
                ret.add(kvList);
            }

            return ret;
        }

        /**
         * Throws RuntimeException if config is incomplete.
         *
         * @param configIn
         */
        private void checkConfig(Map<String, String> configIn) {
            if (!configIn.containsKey(HTableFactory.FAMILY_TAG) ||
                    !configIn.containsKey(HTableFactory.TABLE_NAME_TAG) ||
                    !configIn.containsKey(HTableFactory.ZOOKEEPER_QUORUM_TAG)) {
                String errorMsg = "Error while doing HBase operation with config " + configIn + " ; Config is missing for: "
                        + HTableFactory.TABLE_NAME_TAG + " or " + HTableFactory.ZOOKEEPER_QUORUM_TAG + " or "
                        + HTableFactory.ZOOKEEPER_QUORUM_TAG;
                LOG.error(errorMsg);
                throw new RuntimeException(errorMsg);
            }
        }
    }

    private static enum MultiColumnPutUDAFCounter {
        NULL_KEY_INSERT_FAILURE, NULL_VALUES_INSERT_FAILURE, NUMBER_OF_SUCCESSFUL_PUTS, NUMBER_OF_BATCH_OPERATIONS;
    }

    private static enum ColumnType {
        DOUBLE("d"), INT("i"), LONG("l"), STRING("s");

        private final String columnType;

        private static final Map<String, ColumnType> TYPES = new HashMap<String, ColumnType>();

        static {
           TYPES.put(DOUBLE.columnType, DOUBLE);
           TYPES.put(INT.columnType, INT);
           TYPES.put(LONG.columnType, LONG);
           TYPES.put(STRING.columnType, STRING);
        }

        private ColumnType(String type) {
            this.columnType = type;
        }

        public String getColunType() {
            return this.columnType;
        }

        public static ColumnType fromColumnType(String type) {
            return TYPES.get(type);
        }
    }
}
