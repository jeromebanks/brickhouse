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
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.HashMap;
import java.util.Map;


@Description(name = "union",
        value = "_FUNC_(x) - Returns a map which contains the union of an aggregation of maps "
)
public class UnionUDAF extends AbstractGenericUDAFResolver {


    /// Snarfed from Hives CollectSet UDAF

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        // TODO Auto-generated method stub
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "One argument is expected; either a map or an array.");
        }
        TypeInfo paramType = parameters[0];
        if (paramType.getCategory() == Category.MAP) {
            return new MapUnionUDAFEvaluator();
        } else {
            //// Only maps for now
            throw new UDFArgumentTypeException(0, " Only maps supported for now ");
            ///return new ArrayUnionUDAFEvaluator();
        }
    }


    public static class MapUnionUDAFEvaluator extends GenericUDAFEvaluator {
        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
        private MapObjectInspector inputMapOI;
        private ObjectInspector inputKeyOI;
        private ObjectInspector inputValOI;
        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
        // of objs)
        private StandardMapObjectInspector moi;
        private StandardMapObjectInspector internalMergeOI;


        static class MapAggBuffer implements AggregationBuffer {
            HashMap<Object, Object> collectMap = new HashMap<Object, Object>();
        }

        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            // init output object inspectors
            // The output of a partial aggregation is a list
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputMapOI = (MapObjectInspector) parameters[0];

                inputKeyOI = inputMapOI.getMapKeyObjectInspector();
                inputValOI = inputMapOI.getMapValueObjectInspector();

                return ObjectInspectorFactory.getStandardMapObjectInspector(
                        ObjectInspectorUtils.getStandardObjectInspector(inputKeyOI),
                        ObjectInspectorUtils.getStandardObjectInspector(inputValOI));
            } else {
                if (!(parameters[0] instanceof StandardMapObjectInspector)) {
                    inputKeyOI = (PrimitiveObjectInspector) ObjectInspectorUtils
                            .getStandardObjectInspector(parameters[0]);
                    inputValOI = ObjectInspectorUtils
                            .getStandardObjectInspector(parameters[0]);
                    return (StandardMapObjectInspector) ObjectInspectorFactory
                            .getStandardMapObjectInspector(inputKeyOI, inputValOI);
                } else {
                    internalMergeOI = (StandardMapObjectInspector) parameters[0];
                    inputKeyOI = (PrimitiveObjectInspector) internalMergeOI.getMapKeyObjectInspector();
                    inputValOI = internalMergeOI.getMapValueObjectInspector();
                    moi = (StandardMapObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
                    return moi;
                }
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AggregationBuffer buff = new MapAggBuffer();
            reset(buff);
            return buff;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            Object mpObj = parameters[0];

            if (mpObj != null) {
                MapAggBuffer myagg = (MapAggBuffer) agg;
                Map mp = inputMapOI.getMap(mpObj);
                for (Object k : mp.keySet()) {
                    Object v = mp.get(k);
                    putIntoSet(k, v, myagg);
                }
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            MapAggBuffer myagg = (MapAggBuffer) agg;
            HashMap<Object, Object> partialResult = (HashMap<Object, Object>) internalMergeOI.getMap(partial);
            for (Object i : partialResult.keySet()) {
                putIntoSet(i, partialResult.get(i), myagg);
            }
        }

        @Override
        public void reset(AggregationBuffer buff) throws HiveException {
            MapAggBuffer arrayBuff = (MapAggBuffer) buff;
            arrayBuff.collectMap = new HashMap<Object, Object>();
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MapAggBuffer myagg = (MapAggBuffer) agg;
            HashMap<Object, Object> ret = new HashMap<Object, Object>(myagg.collectMap);
            return ret;

        }

        private void putIntoSet(Object key, Object val, MapAggBuffer myagg) {
            Object keyCopy = ObjectInspectorUtils.copyToStandardObject(key, this.inputKeyOI);
            Object valCopy = ObjectInspectorUtils.copyToStandardObject(val, this.inputValOI);

            myagg.collectMap.put(keyCopy, valCopy);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            MapAggBuffer myagg = (MapAggBuffer) agg;
            HashMap<Object, Object> ret = new HashMap<Object, Object>(myagg.collectMap);
            return ret;
        }
    }


}
