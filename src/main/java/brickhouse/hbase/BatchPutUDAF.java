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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

/**
 * Retrieve from HBase by doing bulk s from an aggregate function call.
 */

@Description(name = "hbase_batch_put",
        value = "_FUNC_(config_map, key, value) - Perform batch HBase updates of a table "
)
public class BatchPutUDAF extends AbstractGenericUDAFResolver {
    private static final Logger LOG = Logger.getLogger(BatchPutUDAF.class);

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        for (int i = 0; i < parameters.length; ++i) {
            LOG.info(" BATCH PUT PARAMETERS : " + i + " -- " + parameters[i].getTypeName() + " cat = " + parameters[i].getCategory());
            System.out.println(" BATCH PUT PARAMETERS : " + i + " -- " + parameters[i].getTypeName() + " cat = " + parameters[i].getCategory());
        }

        return new BatchPutUDAFEvaluator();
    }

    public static class BatchPutUDAFEvaluator extends GenericUDAFEvaluator {
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

        public class PutBuffer implements AggregationBuffer {
            public List<Put> putList;

            public PutBuffer() {
            }

            public void reset() {
                putList = new ArrayList<Put>();
            }

			public void addKeyValue( byte[] key, byte[] val) throws HiveException{
                Put thePut = new Put(key);
                //Disable WAL writes when specified in config map
                if(disableWAL)
                  thePut.setDurability(Durability.SKIP_WAL);

                thePut.add(getFamily(), getQualifier(), val);
                putList.add(thePut);
            }
        }


        private byte[] getFamily() {
            String famStr = configMap.get(HTableFactory.FAMILY_TAG);
            return famStr.getBytes();
        }

        private byte[] getQualifier() {
            String famStr = configMap.get(HTableFactory.QUALIFIER_TAG);
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
		private PrimitiveObjectInspector inputValOI;
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
				HTableFactory.checkConfig( configMap);
				
				
				inputKeyOI = (PrimitiveObjectInspector) parameters[1];
				inputValOI = (PrimitiveObjectInspector) parameters[2];
				
				
				try {
					LOG.info(" Initializing HTable ");
					table = HTableFactory.getHTable( configMap);
					
					if(configMap.containsKey(BATCH_SIZE_TAG)) {
						batchSize = Integer.parseInt( configMap.get( BATCH_SIZE_TAG));
					}

                    if(configMap.containsKey(DISABLE_AUTO_FLUSH)) {
                        disableAutoFlush = Boolean.valueOf( configMap.get( DISABLE_AUTO_FLUSH));
                        LOG.info("Disabling auto flush on hbase puts");
                    }

                    if (configMap.containsKey(DISABLE_WAL)) {
                        disableWAL = Boolean.valueOf(configMap.get(DISABLE_WAL));
                        LOG.info("Disabling WAL writes on hbase puts");
                    }

                    if (configMap.containsKey(WRITE_BUFFER_SIZE_MB)) {
                        writeBufferSizeBytes = Integer.parseInt(configMap.get(WRITE_BUFFER_SIZE_MB)) * 1024 * 1024;
                        LOG.info("Setting habase write buffer size to: " + writeBufferSizeBytes);
                    }
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
										PrimitiveObjectInspectorFactory.javaStringObjectInspector ) );
			} else {
				/// Otherwise return a message
				return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			}
		}

		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			PutBuffer buff= new PutBuffer();
			reset(buff);
			return buff;
		}


        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            byte[] key = HTableFactory.getByteArray(parameters[1], inputKeyOI);
            byte[] val = HTableFactory.getByteArray(parameters[2], inputValOI);

            if (key != null && val != null) {
                PutBuffer kvBuff = (PutBuffer) agg;
                kvBuff.addKeyValue(key, val);
                if (kvBuff.putList.size() >= batchSize) {
                    batchUpdate(kvBuff, false);
                }
            } else {
                getReporter().getCounter(BatchPutUDAFCounter.NULL_KEY_OR_VALUE_INSERT_FAILURE).increment(1);
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
                if (kvBuff.putList.size() > 0)
                    LOG.info(" Doing Batch Put " + kvBuff.putList.size() + " records; Total put records = " + numPutRecords + " ; Start = " + (new String(kvBuff.putList.get(0).getRow())) + " ; End = " + (new String(kvBuff.putList.get(kvBuff.putList.size() - 1).getRow())));
                else
                    LOG.info(" Doing Batch Put with ZERO 0 records");

                getReporter().getCounter(BatchPutUDAFCounter.NUMBER_OF_SUCCESSFUL_PUTS).increment(kvBuff.putList.size());
                getReporter().getCounter(BatchPutUDAFCounter.NUMBER_OF_BATCH_OPERATIONS).increment(1);
                kvBuff.putList.clear();
            } catch (IOException e) {
                throw new HiveException(e);
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
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
            String qualifier = ((StringObjectInspector) (subListOI.getListElementObjectInspector())).getPrimitiveJavaObject(first.get(3));
            configMap.put(HTableFactory.QUALIFIER_TAG, qualifier);
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
                String val = ((StringObjectInspector) (subListOI.getListElementObjectInspector())).getPrimitiveJavaObject(kvList.get(1));

                myagg.addKeyValue(key.getBytes(), val.getBytes());

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
            tname.add(configMap.get(HTableFactory.QUALIFIER_TAG));

            for (Entry<String, String> entry : configMap.entrySet()) {
                if (!entry.getKey().equals(HTableFactory.TABLE_NAME_TAG)
                        && !entry.getKey().equals(HTableFactory.ZOOKEEPER_QUORUM_TAG)
                        && !entry.getKey().equals(HTableFactory.FAMILY_TAG)
                        && !entry.getKey().equals(HTableFactory.QUALIFIER_TAG)) {

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
    }

    private static enum BatchPutUDAFCounter {
        NULL_KEY_OR_VALUE_INSERT_FAILURE, NUMBER_OF_SUCCESSFUL_PUTS, NUMBER_OF_BATCH_OPERATIONS;
    }
}
