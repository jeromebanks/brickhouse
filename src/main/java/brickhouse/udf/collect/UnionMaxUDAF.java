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
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;


/**
 * UDAF to merge a union of maps,
 * but only hold on the keys with the top 20 values
 */

@Description(name = "union_max",
        value = "_FUNC_(x,  n) - Returns an map of the union of maps of max N elements in the aggregation group "
)
public class UnionMaxUDAF extends AbstractGenericUDAFResolver {
    private static final Logger LOG = Logger.getLogger(UnionMaxUDAF.class);
    public static int DEFAULT_MAX_VALUES = 20;


    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        return new MapCollectMaxUDAFEvaluator();
    }


    public static class MapCollectMaxUDAFEvaluator extends GenericUDAFEvaluator {
        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
        private PrimitiveObjectInspector inputKeyOI;
        private PrimitiveObjectInspector inputValOI; /// XXX Support nested values instead of just primitives as values
        private MapObjectInspector inputMapOI;
        private IntObjectInspector nvOI;
        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
        // of objs)
        private StandardMapObjectInspector moi;
        private StandardMapObjectInspector internalMergeOI;


        public static class SortedKeyValue implements Comparable {
            private String key;
            private Double value;


            public SortedKeyValue(String key, Double val) {
                this.key = key;
                this.value = val;
            }

            @Override
            public boolean equals(Object other) {
                if (!(other instanceof SortedKeyValue)) {
                    return false;
                }
                SortedKeyValue otherKV = (SortedKeyValue) other;
                if (key.equals(otherKV.key)) {
                    return true;
                } else {
                    return false;
                }

            }

            public String getKey() {
                return key;
            }

            public Double getValue() {
                return value;
            }

            @Override
            public int compareTo(Object arg1) {
                SortedKeyValue kv0 = (SortedKeyValue) this;
                SortedKeyValue kv1 = (SortedKeyValue) arg1;

                if (kv0.value != kv1.value) {
                    if (kv0.value > kv1.value) {
                        return -1;
                    } else {
                        if (kv0.value < kv1.value) {
                            return 1;
                        }
                    }
                    return kv0.key.compareTo(kv1.key);
                } else {
                    return kv0.key.compareTo(kv1.key);
                }
            }
        }

        static class MapAggBuffer implements AggregationBuffer {
            private TreeSet<SortedKeyValue> sortedValues = new TreeSet<SortedKeyValue>();
            private int numValues = DEFAULT_MAX_VALUES;

            public void setNumValues(int nv) {
                numValues = nv;
            }

            public void addValue(String key, Double value) {
                if (sortedValues.size() < numValues) {
                    sortedValues.add(new SortedKeyValue(key, value));
                } else {
                    SortedKeyValue minValue = sortedValues.last();
                    if (value > minValue.getValue()) {
                        sortedValues.remove(minValue);
                        sortedValues.add(new SortedKeyValue(key, value));
                    }
                }
            }

            public void fromMap(Map<Object, Object> fromMap) {
                for (Object kObj : fromMap.keySet()) {
                    Object val = fromMap.get(kObj);
                    addValue((String) kObj, (Double) val);
                }
            }

            public Map<String, Double> getValueMap() {
                LinkedHashMap<String, Double> reverseOrderMap = new LinkedHashMap<String, Double>();
                for (SortedKeyValue kv : sortedValues) {
                    reverseOrderMap.put(kv.key, kv.value);
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
            LOG.info(" UnionMaxUDAF.init() - Mode= " + m.name());
            for (int i = 0; i < parameters.length; ++i) {
                LOG.info(" ObjectInspector[ " + i + " ] = " + parameters[0]);
            }
            if (parameters.length > 1) {
                nvOI = (IntObjectInspector) parameters[1];
            }

            // init output object inspectors
            // The output of a partial aggregation is a map
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputMapOI = (MapObjectInspector) parameters[0];
                inputKeyOI = (PrimitiveObjectInspector) inputMapOI.getMapKeyObjectInspector();
                inputValOI = (PrimitiveObjectInspector) inputMapOI.getMapValueObjectInspector();

                /**
                 return ObjectInspectorFactory.getStandardMapObjectInspector(
                 ObjectInspectorUtils.getStandardObjectInspector(inputKeyOI),
                 ObjectInspectorUtils.getStandardObjectInspector(inputValOI) );
                 **/
            } else {
                if (!(parameters[0] instanceof StandardMapObjectInspector)) {
                    LOG.info(" Not a standard map OjbectInspector ");
                    inputKeyOI = (PrimitiveObjectInspector) ObjectInspectorUtils
                            .getStandardObjectInspector(parameters[0]);
                    inputValOI = (PrimitiveObjectInspector) ObjectInspectorUtils
                            .getStandardObjectInspector(parameters[1]);
                    /**
                     return (StandardMapObjectInspector) ObjectInspectorFactory
                     .getStandardMapObjectInspector(inputKeyOI, inputValOI);
                     **/
                } else {
                    internalMergeOI = (StandardMapObjectInspector) parameters[0];
                    inputKeyOI = (PrimitiveObjectInspector) internalMergeOI.getMapKeyObjectInspector();
                    inputValOI = (PrimitiveObjectInspector) internalMergeOI.getMapValueObjectInspector();
                    /**
                     moi =  (StandardMapObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
                     return moi;
                     **/
                }
            }
            return ObjectInspectorFactory.getStandardMapObjectInspector(
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                    PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
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
            Map inMap = inputMapOI.getMap(parameters[0]);
            for (Object k : inMap.keySet()) {
                Object v = inMap.get(k);
                if (k == null || v == null) {
                    throw new HiveException("Kay or value is null.  k = " + k + " , v = " + v);
                }

                if (k != null) {
                    MapAggBuffer myagg = (MapAggBuffer) agg;

                    if (parameters.length > 1) {
                        Object numValsObj = parameters[1];
                        int nv = nvOI.get(numValsObj);
                        myagg.setNumValues(nv);

                    }
                    putIntoSet(k, v, myagg);
                }
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
            StringObjectInspector strInsp = (StringObjectInspector) this.inputKeyOI;
            DoubleObjectInspector dblInsp = (DoubleObjectInspector) this.inputValOI;

            String keyCopy = strInsp.getPrimitiveJavaObject(key);
            Double valCopy = dblInsp.get(val);

            myagg.addValue(keyCopy, valCopy);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {

            MapAggBuffer myagg = (MapAggBuffer) agg;
            Map<String, Double> vals = myagg.getValueMap();
            return vals;
        }
    }


}
