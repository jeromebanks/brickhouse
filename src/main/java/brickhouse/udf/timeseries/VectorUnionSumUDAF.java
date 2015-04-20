package brickhouse.udf.timeseries;
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
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


@Description(name = "union_vector_sum",
        value = "_FUNC_(x) - Aggregate adding vectors together "
)
public class VectorUnionSumUDAF extends AbstractGenericUDAFResolver {


    /// Snarfed from Hives CollectSet UDAF

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        // TODO Auto-generated method stub
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Vector sum takes one argument");
        }
        if (parameters[0].getCategory() == Category.LIST) {
            return new VectorArraySumUDAFEvaluator();
        } else if (parameters[0].getCategory() == Category.MAP) {
            return new VectorMapSumUDAFEvaluator();
        } else {
            throw new UDFArgumentTypeException(0, " vector_union_sum aggregates either arrays or maps");
        }
    }

    public static class VectorArraySumUDAFEvaluator extends GenericUDAFEvaluator {
        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data, an array
        private ListObjectInspector inputOI;
        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
        //  ( sum of arrays, or arrays)
        private StandardListObjectInspector stdListOI;


        static class VectorArrayAggBuffer implements AggregationBuffer {
            ArrayList<Double> sumArray = new ArrayList<Double>();
        }

        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            inputOI = (ListObjectInspector) parameters[0];
            if (inputOI.getListElementObjectInspector().getCategory() != Category.PRIMITIVE
                    || !NumericUtil.isNumericCategory(
                    ((PrimitiveObjectInspector) inputOI.getListElementObjectInspector()).getPrimitiveCategory())) {
                throw new HiveException("Vector values must be numeric.");
            }
            /// always return the standard list of doubles
            stdListOI = ObjectInspectorFactory
                    .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
            return stdListOI;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AggregationBuffer buff = new VectorArrayAggBuffer();
            reset(buff);
            return buff;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            Object p = parameters[0];

            if (p != null) {
                VectorArrayAggBuffer myagg = (VectorArrayAggBuffer) agg;
                addVector(p, myagg, inputOI);
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            VectorArrayAggBuffer myagg = (VectorArrayAggBuffer) agg;
            addVector(partial, myagg, this.inputOI);
        }

        @Override
        public void reset(AggregationBuffer buff) throws HiveException {
            VectorArrayAggBuffer arrayBuff = (VectorArrayAggBuffer) buff;
            arrayBuff.sumArray = new ArrayList<Double>();
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            VectorArrayAggBuffer myagg = (VectorArrayAggBuffer) agg;
            return myagg.sumArray;
        }

        private void addVector(Object listObj, VectorArrayAggBuffer myagg, ListObjectInspector inputOI) {
            int listLen = inputOI.getListLength(listObj);
            if (listLen > myagg.sumArray.size())
                myagg.sumArray.ensureCapacity(listLen);

            for (int i = 0; i < listLen; ++i) {
                Object listElem = inputOI.getListElement(listObj, i);
                double listElemDbl = NumericUtil.getNumericValue(
                        (PrimitiveObjectInspector) inputOI.getListElementObjectInspector(), listElem);
                Double oldVal = myagg.sumArray.get(i);
                if (oldVal != null) {
                    myagg.sumArray.set(i, oldVal + listElemDbl);
                } else {
                    myagg.sumArray.set(i, listElemDbl);
                }
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            VectorArrayAggBuffer myagg = (VectorArrayAggBuffer) agg;
            return myagg.sumArray;
        }
    }

    public static class VectorMapSumUDAFEvaluator extends GenericUDAFEvaluator {
        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data, an array
        private MapObjectInspector inputOI;
        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
        //  ( sum of arrays, or arrays)
        private StandardMapObjectInspector stdMapOI;


        static class VectorMapAggBuffer implements AggregationBuffer {
            Map<Object, Double> sumMap = new HashMap<Object, Double>();
        }

        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            inputOI = (MapObjectInspector) parameters[0];
            if (inputOI.getMapKeyObjectInspector().getCategory() != Category.PRIMITIVE) {
                throw new HiveException("Vector map keys must be a primitive.");
            }
            if (inputOI.getMapValueObjectInspector().getCategory() != Category.PRIMITIVE
                    || !NumericUtil.isNumericCategory(
                    ((PrimitiveObjectInspector) inputOI.getMapValueObjectInspector()).getPrimitiveCategory())) {
                throw new HiveException("Vector values must be numeric.");
            }
            stdMapOI = ObjectInspectorFactory.
                    getStandardMapObjectInspector(
                            ObjectInspectorUtils.getStandardObjectInspector(inputOI.getMapKeyObjectInspector(),
                                    ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA),
                            PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
            //// XXX make return type  numeric type of input,
            //// not doubles...
            return stdMapOI;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AggregationBuffer buff = new VectorMapAggBuffer();
            reset(buff);
            return buff;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            Object p = parameters[0];

            if (p != null) {
                VectorMapAggBuffer myagg = (VectorMapAggBuffer) agg;
                addVectorMap(p, myagg, inputOI);
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            VectorMapAggBuffer myagg = (VectorMapAggBuffer) agg;
            addVectorMap(partial, myagg, this.inputOI);
        }

        @Override
        public void reset(AggregationBuffer buff) throws HiveException {
            VectorMapAggBuffer arrayBuff = (VectorMapAggBuffer) buff;
            arrayBuff.sumMap = new HashMap<Object, Double>();
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            VectorMapAggBuffer myagg = (VectorMapAggBuffer) agg;
            return myagg.sumMap;
        }

        private void addVectorMap(Object mapObj, VectorMapAggBuffer myagg, MapObjectInspector inputOI) {
            Map uninspMap = inputOI.getMap(mapObj);
            for (Object uninspKey : uninspMap.keySet()) {
                Object stdKey = ObjectInspectorUtils.copyToStandardJavaObject(uninspKey,
                        inputOI.getMapKeyObjectInspector());

                double stdVal = NumericUtil.getNumericValue((PrimitiveObjectInspector) inputOI.getMapValueObjectInspector(), uninspMap.get(uninspKey));
                if (myagg.sumMap.containsKey(stdKey)) {
                    double prevVal = myagg.sumMap.get(stdKey);
                    myagg.sumMap.put(stdKey, prevVal + stdVal);
                } else {
                    myagg.sumMap.put(stdKey, stdVal);
                }
            }
        }


        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            VectorMapAggBuffer myagg = (VectorMapAggBuffer) agg;
            return myagg.sumMap;
        }
    }


}
