package brickhouse.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
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
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by casselstine on 6/8/15.
 */

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



/**
 * Retrieve from HBase by doing bulk s from an aggregate function call.
 */
/**
 *  Perform a batch delete into HBase given an hbase key, array of column qualifiers and array of values
 *
 *  This UDF by default does a put that can override initial cell values of hbase records, however, that behaviour can
 *  be changed when the "brickhouse.overwrite.cell" config property is set to "n" in the config map. In such case,
 *  this UDF will perform an hbase GET first to find out if a given record exists in hbase before inserting it.
 *
 *  Usage example:
 *   hbase_batch_delete( map("table_name", "xunit_segment",
 *                                "hbase.zookeeper.quorum" , "dahdp2jt01.tag-dev.com,dahdp2nn01.tag-dev.com,dahdp2rm01.tag-dev.com",
 *                                "zookeeper.znode.parent", "/hbase-unsecure",
 *                                "brickhouse.overwrite.cell", "n"), xunit)  -- hbase config map
 *
 */

@Description(name = "hbase_batch_delete",
        value = "_FUNC_(config_map, key) - Perform batch HBase updates of a table "
)
public class BatchDeleteUDAF extends AbstractGenericUDAFResolver {
    private static final Logger LOG = Logger.getLogger(BatchDeleteUDAF.class);

    private static Map<String, HTable> htableMap = new HashMap<String,HTable>();
    private static Configuration hbConfig;

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        for (int i = 0; i < parameters.length; ++i) {
            LOG.info(" BATCH DELETE PARAMETERS : " + i + " -- " + parameters[i].getTypeName() + " cat = " + parameters[i].getCategory());
            System.out.println(" BATCH DELETE PARAMETERS : " + i + " -- " + parameters[i].getTypeName() + " cat = " + parameters[i].getCategory());
        }

        return new BatchDeleteUDAFEvaluator();
    }

    public static class BatchDeleteUDAFEvaluator extends GenericUDAFEvaluator {
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
            } catch (Exception e) {
                throw new HiveException("Error while accessing Hadoop Counters", e);
            }
        }

        public class DeleteBuffer implements AggregationBuffer {
            public List<Delete> deleteList;

            public DeleteBuffer() {
            }

            public void reset() {
                deleteList = new ArrayList<Delete>();
            }

            public void addKey( byte[] key) throws HiveException{
                Delete theDelete = new Delete(key);
                //Disable WAL writes when specified in config map
                if(disableWAL)
                    theDelete.setDurability(Durability.SKIP_WAL);

                deleteList.add(theDelete);
                getReporter().getCounter(BatchDeleteUDAFCounter.DELETE_ADDED).increment(1);
                System.out.println("Added delete:" + key.toString());

            }
        }


        private int batchSize = 10000;
        private int numDeleteRecords = 0;
        private boolean disableWAL = false;
        private boolean disableAutoFlush = false;
        private int writeBufferSizeBytes = 0;

        public static final String BATCH_SIZE_TAG = "batch_size";
//        public static final String DISABLE_WAL = "disable_wal";
        public static final String DISABLE_AUTO_FLUSH = "disable_auto_flush";
//        public static final String WRITE_BUFFER_SIZE_MB = "write_buffer_size_mb";

        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
        private PrimitiveObjectInspector inputKeyOI;
        //        private PrimitiveObjectInspector inputValOI;
        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
        // of objs)
        private StandardListObjectInspector listKVOI;
        private Map<String,String> configMap;

        private HTable table;


        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            // init output object inspectors
            ///  input will be key, value and batch size
            LOG.info(" Init mode = " + m );
            System.out.println(" Init mode = " + m );
            System.out.println(" parameters =  = " + parameters + " Length = " + parameters.length );
            configMap = new HashMap<String,String>();
            for( int k=0; k< parameters.length; ++k) {
                LOG.info( "Param " + k + " is " + parameters[k]);
                System.out.println( "Param " + k + " is " + parameters[k]);
            }

            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE ) {
                configMap = HTableFactory.getConfigFromConstMapInspector(parameters[0]);
                checkConfig( configMap);


                inputKeyOI = (PrimitiveObjectInspector) parameters[1];

                try {
                    LOG.info(" Initializing HTable ");
                    table = HTableFactory.getHTable(configMap);

                    if(configMap.containsKey(BATCH_SIZE_TAG)) {
                        batchSize = Integer.parseInt( configMap.get( BATCH_SIZE_TAG));
                    }

                    if(configMap.containsKey(DISABLE_AUTO_FLUSH)) {
                        disableAutoFlush = Boolean.valueOf( configMap.get( DISABLE_AUTO_FLUSH));
                        LOG.info("Disabling auto flush on hbase deletes");
                    }

//                    if (configMap.containsKey(DISABLE_WAL)) {
//                        disableWAL = Boolean.valueOf(configMap.get(DISABLE_WAL));
//                        LOG.info("Disabling WAL writes on hbase deletes");
//                    }
//
//                    if (configMap.containsKey(WRITE_BUFFER_SIZE_MB)) {
//                        writeBufferSizeBytes = Integer.parseInt(configMap.get(WRITE_BUFFER_SIZE_MB)) * 1024 * 1024;
//                        LOG.info("Setting habase write buffer size to: " + writeBufferSizeBytes);
//                    }
                } catch (IOException e) {
                    throw new HiveException(e);
                }

            } else {
                listKVOI = (StandardListObjectInspector) parameters[0];
            }

            if( m == Mode.PARTIAL1 || m  == Mode.PARTIAL2) {
                return ObjectInspectorFactory
                        .getStandardListObjectInspector(
                                ObjectInspectorFactory.getStandardListObjectInspector(
                                        PrimitiveObjectInspectorFactory.javaStringObjectInspector));
            } else {
                /// Otherwise return a message
                return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            DeleteBuffer buff= new DeleteBuffer();
            reset(buff);
            return buff;
        }


        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            byte[] key = HTableFactory.getByteArray(parameters[1], inputKeyOI);

            if (key != null) {
                DeleteBuffer kvBuff = (DeleteBuffer) agg;
                kvBuff.addKey(key);
                if (kvBuff.deleteList.size() >= batchSize) {
                    batchUpdate(kvBuff, false);
                }
            } else {
                getReporter().getCounter(BatchDeleteUDAFCounter.NULL_KEY_DELETE_FAILURE).increment(1);
            }
        }


        protected void batchUpdate(DeleteBuffer kvBuff, boolean flushCommits) throws HiveException {
            try {

                HTable htable = HTableFactory.getHTable(configMap);
                // Disable auto flush when specified so in the config map
                if (disableAutoFlush)
                    htable.setAutoFlushTo(false);

                // Overwrite the write buffer size when config map specifies to do so
                if (writeBufferSizeBytes > 0)
                    htable.setWriteBufferSize(writeBufferSizeBytes);
                System.out.println("deleting" + kvBuff.deleteList + "size" + kvBuff.deleteList.size());
                if (flushCommits)
                    htable.flushCommits();
                numDeleteRecords += kvBuff.deleteList.size();
                if (kvBuff.deleteList.size() > 0)
                    LOG.info(" Doing Batch Delete " + kvBuff.deleteList.size() + " records; Total delete records = " + numDeleteRecords + " ; Start = " + (new String(kvBuff.deleteList.get(0).getRow())) + " ; End = " + (new String(kvBuff.deleteList.get(kvBuff.deleteList.size() - 1).getRow())));
                else
                    LOG.info(" Doing Batch Delete with ZERO 0 records");

                getReporter().getCounter(BatchDeleteUDAFCounter.NUMBER_OF_SUCCESSFUL_DELETES).increment(kvBuff.deleteList.size());
                getReporter().getCounter(BatchDeleteUDAFCounter.NUMBER_OF_BATCH_OPERATIONS).increment(1);
                htable.delete(kvBuff.deleteList);
                kvBuff.deleteList.clear();
            } catch (IOException e) {
                throw new HiveException(e);
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            DeleteBuffer myagg = (DeleteBuffer) agg;
            List<Object> partialResult = (List<Object>) this.listKVOI.getList(partial);
            ListObjectInspector subListOI = (ListObjectInspector) listKVOI.getListElementObjectInspector();

            List first = subListOI.getList(partialResult.get(0));
            String tableName = ((StringObjectInspector) (subListOI.getListElementObjectInspector())).getPrimitiveJavaObject(first.get(0));
            configMap.put(HTableFactory.TABLE_NAME_TAG, tableName);
            String zookeeper = ((StringObjectInspector) (subListOI.getListElementObjectInspector())).getPrimitiveJavaObject(first.get(1));
            configMap.put(HTableFactory.ZOOKEEPER_QUORUM_TAG, zookeeper);

            //// Include arbitrary configurations, by adding strings of the form k=v
            for (int j = 4; j < first.size(); ++j) {
                String kvStr = ((StringObjectInspector) (subListOI.getListElementObjectInspector())).getPrimitiveJavaObject(first.get(j));
                String[] kvArr = kvStr.split("=");
                if (kvArr.length == 2) {
                    configMap.put(kvArr[0], kvArr[1]);
                }
            }

            for (int i = 1; i < partialResult.size(); ++i) {

                List kvList = subListOI.getList(partialResult.get(i));
                String key = ((StringObjectInspector) (subListOI.getListElementObjectInspector())).getPrimitiveJavaObject(kvList.get(0));

                myagg.addKey(key.getBytes());

            }

            if (myagg.deleteList.size() >= batchSize) {
                batchUpdate(myagg, false);
            }
        }

        @Override
        public void reset(AggregationBuffer buff) throws HiveException {
            DeleteBuffer deleteBuffer = (DeleteBuffer) buff;
            deleteBuffer.reset();
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            DeleteBuffer myagg = (DeleteBuffer) agg;
            batchUpdate(myagg, true);
            return "Finished Batch updates ; Num Deletes = " + numDeleteRecords;

        }


        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            DeleteBuffer myagg = (DeleteBuffer) agg;

            ArrayList<List<String>> ret = new ArrayList<List<String>>();
            ArrayList tname = new ArrayList<String>();
            tname.add(configMap.get(HTableFactory.TABLE_NAME_TAG));
            tname.add(configMap.get(HTableFactory.ZOOKEEPER_QUORUM_TAG));

            for (Map.Entry<String, String> entry : configMap.entrySet()) {
                if (!entry.getKey().equals(HTableFactory.TABLE_NAME_TAG)
                        && !entry.getKey().equals(HTableFactory.ZOOKEEPER_QUORUM_TAG)){

                    tname.add(entry.getKey());
                }
            }
            ret.add(tname);

            for (Delete theDelete : myagg.deleteList) {
                ArrayList<String> kvList = new ArrayList<String>();
                kvList.add(new String(theDelete.getRow()));
                ret.add(kvList);
            }

            return ret;
        }
        private void checkConfig(Map<String, String> configIn) {
            if (!configIn.containsKey(HTableFactory.TABLE_NAME_TAG) ||
                    !configIn.containsKey(HTableFactory.ZOOKEEPER_QUORUM_TAG))
            {
                String errorMsg = "Error while doing HBase operation with config " + configIn + " ; Config is missing for: "
                        + HTableFactory.TABLE_NAME_TAG + " or " + HTableFactory.ZOOKEEPER_QUORUM_TAG;
                LOG.error(errorMsg);
                throw new RuntimeException(errorMsg);
            }
        }
    }

    private static enum BatchDeleteUDAFCounter {
        NULL_KEY_DELETE_FAILURE, NUMBER_OF_SUCCESSFUL_DELETES, NUMBER_OF_BATCH_OPERATIONS, DELETE_ADDED;
    }

}
