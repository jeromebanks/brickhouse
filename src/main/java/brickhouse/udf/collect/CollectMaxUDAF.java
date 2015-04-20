package brickhouse.udf.collect;
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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;

@Description(name = "collect_max",
        value = "_FUNC_(x, val, n) - Returns an map of the max N numeric values in the aggregation group "
)
public class CollectMaxUDAF extends AbstractGenericUDAFResolver {
    public static final Logger LOG = Logger.getLogger(CollectMaxUDAF.class);
    public static int DEFAULT_MAX_VALUES = 20;

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        return new MapCollectMaxUDAFEvaluator();
    }

    public static class MapCollectMaxUDAFEvaluator extends GenericUDAFEvaluator {
        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
        private PrimitiveObjectInspector inputKeyOI;
        private PrimitiveObjectInspector inputValOI;
        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
        // of objs)
        private StandardMapObjectInspector internalMergeOI;
        private boolean descending = true;
        private int numValues = DEFAULT_MAX_VALUES;


        public MapCollectMaxUDAFEvaluator() {
            this(true);
        }

        public MapCollectMaxUDAFEvaluator(boolean desc) {
            this.descending = desc;
        }

        public class SortedKeyValue implements Comparable {
            private Object keyObj;
            private Object valObj;


            public SortedKeyValue(Object keyObj, Object valObj) {
                this.keyObj = keyObj;
                this.valObj = valObj;
            }

            @Override
            public boolean equals(Object other) {
                if (!(other instanceof SortedKeyValue)) {
                    return false;
                }
                SortedKeyValue otherKV = (SortedKeyValue) other;
                if (getKey().equals(otherKV.getKey())) {
                    return true;
                } else {
                    return false;
                }
            }

            public Object getKey() {
                return (keyObj == null) ? null : inputKeyOI.getPrimitiveJavaObject(keyObj);
            }

            public Double getValue() {
                return (valObj == null ? null : ((Number) inputValOI.getPrimitiveJavaObject(valObj)).doubleValue());
            }

            public String toString() {
                return getKey() + ":=" + getValue();
            }

            @Override
            public int compareTo(Object arg1) {
                SortedKeyValue otherKV = (SortedKeyValue) arg1;
                int cmp = compareKV(otherKV);
                return (descending ? cmp : -1 * cmp);
            }

            public int compareKV(SortedKeyValue otherKV) {

                double thisNumber = getValue();
                double otherNumber = otherKV.getValue();
                int sigNum = (int) Math.signum(otherNumber - thisNumber);
                if (sigNum != 0) {
                    return sigNum;
                } else {
                    return ((Comparable) getKey()).compareTo(otherKV.getKey());
                }
            }
        }

        class MapAggBuffer implements AggregationBuffer {
            private TreeSet<SortedKeyValue> sortedValues = new TreeSet<SortedKeyValue>();


            public void addValue(Object keyObj, Object valObj) {
                if (sortedValues.size() < numValues) {
                    SortedKeyValue newValue = new SortedKeyValue(inputKeyOI.copyObject(keyObj), inputValOI.copyObject(valObj));
                    sortedValues.add(newValue);
                } else {
                    SortedKeyValue minValue = sortedValues.last();
                    SortedKeyValue biggerValue = new SortedKeyValue(inputKeyOI.copyObject(keyObj), inputValOI.copyObject(valObj));
                    int cmp = biggerValue.compareTo(minValue);
                    if (cmp < 0) {
                        sortedValues.remove(minValue);
                        sortedValues.add(biggerValue);
                    }
                }
            }


            public Map getValueMap() {
                LinkedHashMap<Object, Object> reverseOrderMap = new LinkedHashMap<Object, Object>();
                for (SortedKeyValue kv : sortedValues) {
                    reverseOrderMap.put(kv.keyObj, kv.valObj);
                }
                return reverseOrderMap;
            }

            public void reset() {
                sortedValues.clear();
            }
        }

        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            LOG.error(" CollectMaxUDAF.init() - Mode= " + m.name());
            for (int i = 0; i < parameters.length; ++i) {
                LOG.error(" ObjectInspector[ " + i + " ] = " + parameters[0]);
            }
            if (parameters.length > 2) {
                if (parameters[2] instanceof WritableConstantIntObjectInspector) {
                    WritableConstantIntObjectInspector nvOI = (WritableConstantIntObjectInspector) parameters[2];
                    numValues = nvOI.getWritableConstantValue().get();
                    LOG.info(" Setting number of values to " + numValues);
                } else {
                    throw new HiveException("Number of values must be a constant int.");
                }
            }

            // init output object inspectors
            // The output of a partial aggregation is a map
            if (m == Mode.PARTIAL1) {
                inputKeyOI = (PrimitiveObjectInspector) parameters[0];
                inputValOI = (PrimitiveObjectInspector) parameters[1];
                return ObjectInspectorFactory.getStandardMapObjectInspector(
                        inputKeyOI,
                        inputValOI);
            } else {
                if (!(parameters[0] instanceof StandardMapObjectInspector)) {
                    inputKeyOI = (PrimitiveObjectInspector) parameters[0];
                    inputValOI = (PrimitiveObjectInspector) parameters[1];
                } else {
                    internalMergeOI = (StandardMapObjectInspector) parameters[0];
                    inputKeyOI = (PrimitiveObjectInspector) internalMergeOI.getMapKeyObjectInspector();
                    inputValOI = (PrimitiveObjectInspector) internalMergeOI.getMapValueObjectInspector();
                }
            }
            return ObjectInspectorFactory.getStandardMapObjectInspector(
                    inputKeyOI,
                    inputValOI);
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MapAggBuffer buff = new MapAggBuffer();
            reset(buff);
            return buff;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            Object k = parameters[0];
            Object v = parameters[1];
            if (k == null || v == null) {
                throw new HiveException("Key or value is null.  k = " + k + " , v = " + v);
            }

            if (k != null) {
                MapAggBuffer myagg = (MapAggBuffer) agg;

                putIntoSet(k, v, myagg);
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            MapAggBuffer myagg = (MapAggBuffer) agg;
            Map<Object, Object> partialResult = (Map<Object, Object>) internalMergeOI.getMap(partial);
            for (Object i : partialResult.keySet()) {
                putIntoSet(i, partialResult.get(i), myagg);
            }
        }

        @Override
        public void reset(AggregationBuffer buff) throws HiveException {
            MapAggBuffer arrayBuff = (MapAggBuffer) buff;
            arrayBuff.reset();
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MapAggBuffer myagg = (MapAggBuffer) agg;
            return myagg.getValueMap();

        }

        private void putIntoSet(Object key, Object val, MapAggBuffer myagg) {
            myagg.addValue(key, val);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {

            MapAggBuffer myagg = (MapAggBuffer) agg;
            Map<Object, Object> vals = myagg.getValueMap();
            return vals;
        }
    }


}
