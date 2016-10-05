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


/**
 * Similar to Ruby collect, 
 *   return an array with all the values
 */

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;


@Description(name = "collect_distinct",
        value = "_FUNC_(x) - Returns an array of distinct the elements in the aggregation group "
)
public class CollectDistinctUDAF extends AbstractGenericUDAFResolver {


    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        // TODO Auto-generated method stub
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "One argument is expected to return an Array.");
        }
        return new SetCollectUDAFEvaluator();
        
    }

    public static class SetCollectUDAFEvaluator extends GenericUDAFEvaluator {
        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
        private ObjectInspector inputOI;
        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
        // of objs)
        private StandardListObjectInspector loi;
        private StandardListObjectInspector internalMergeOI;


        static class ArrayAggBuffer implements AggregationBuffer {
            Set collectArray = new HashSet();
        }

        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            // init output object inspectors
            // The output of a partial aggregation is a list
            if (m == Mode.PARTIAL1) {
                inputOI = parameters[0];
                return ObjectInspectorFactory
                        .getStandardListObjectInspector(ObjectInspectorUtils
                                .getStandardObjectInspector(inputOI));
            } else {
                if (!(parameters[0] instanceof StandardListObjectInspector)) {
                    //no map aggregation.
                    inputOI = ObjectInspectorUtils
                            .getStandardObjectInspector(parameters[0]);
                    return (StandardListObjectInspector) ObjectInspectorFactory
                            .getStandardListObjectInspector(inputOI);
                } else {
                    internalMergeOI = (StandardListObjectInspector) parameters[0];
                    inputOI = internalMergeOI.getListElementObjectInspector();
                    loi = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
                    return loi;
                }
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AggregationBuffer buff = new ArrayAggBuffer();
            reset(buff);
            return buff;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            Object p = parameters[0];

            if (p != null) {
                ArrayAggBuffer myagg = (ArrayAggBuffer) agg;
                putIntoSet(p, myagg);
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            ArrayAggBuffer myagg = (ArrayAggBuffer) agg;
            ArrayList<Object> partialResult = (ArrayList<Object>) internalMergeOI.getList(partial);
            for (Object i : partialResult) {
                putIntoSet(i, myagg);
            }
        }

        @Override
        public void reset(AggregationBuffer buff) throws HiveException {
            ArrayAggBuffer arrayBuff = (ArrayAggBuffer) buff;
            arrayBuff.collectArray = new HashSet();
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            ArrayAggBuffer myagg = (ArrayAggBuffer) agg;
            ArrayList<Object> ret = new ArrayList<Object>(myagg.collectArray.size());
            ret.addAll(myagg.collectArray);
            return ret;

        }

        private void putIntoSet(Object p, ArrayAggBuffer myagg) {
            Object pCopy = ObjectInspectorUtils.copyToStandardObject(p,
                    this.inputOI);
            myagg.collectArray.add(pCopy);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            ArrayAggBuffer myagg = (ArrayAggBuffer) agg;
            ArrayList<Object> ret = new ArrayList<Object>(myagg.collectArray.size());
            ret.addAll(myagg.collectArray);
            return ret;
        }
    }
}
