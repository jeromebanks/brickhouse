package brickhouse.udf.sketch;
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
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import java.util.Map;


/**
 * Construct a sketch set by aggregating over a a set of ID's
 */

@Description(name = "sketch_set",
        value = "_FUNC_(x) - Constructs a sketch set to estimate reach for large values  "
)
public class SketchSetUDAF extends AbstractGenericUDAFResolver {
    private static final Logger LOG = Logger.getLogger(SketchSetUDAF.class);
    public static int DEFAULT_SKETCH_SET_SIZE = 5000;
    static String SKETCH_SIZE_STR = "SKETCH_SIZE";


    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {

        if (!parameters[0].getTypeName().equals("string")
                && !parameters[0].getTypeName().equals("bigint")) {
            throw new SemanticException("sketch_set UDAF only takes String or longs as values; not " + parameters[0].getTypeName());
        }
        if ((parameters.length > 1) && !parameters[1].getTypeName().equals("int")) {
            throw new SemanticException("Size of sketch must be an int; Got " + parameters[1].getTypeName());
        }
        return new SketchSetUDAFEvaluator();
    }


    public static class SketchSetUDAFEvaluator extends GenericUDAFEvaluator {
        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
        private StringObjectInspector inputStrOI;
        private MapObjectInspector partialMapOI;
        private LongObjectInspector partialMapHashOI;
        private StringObjectInspector partialMapStrOI;
        private int sketchSetSize = -1;


        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            ///
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                //// iterate() gets called.. string is passed in
                this.inputStrOI = (StringObjectInspector) parameters[0];
                if (parameters.length > 1 && m == Mode.PARTIAL1) {
                    //// get the sketch set size from the second parameters
                    if (!(parameters[1] instanceof ConstantObjectInspector)) {
                        throw new HiveException("Sketch Set size must be a constant");
                    }
                    ConstantObjectInspector sizeOI = (ConstantObjectInspector) parameters[1];
                    this.sketchSetSize = ((IntWritable) sizeOI.getWritableConstantValue()).get();
                } else {
                    sketchSetSize = DEFAULT_SKETCH_SET_SIZE;
                }
            } else { /// Mode m == Mode.PARTIAL2 || m == Mode.FINAL
                /// merge() gets called ... map is passed in ..
                this.partialMapOI = (MapObjectInspector) parameters[0];
                this.partialMapHashOI = (LongObjectInspector) partialMapOI.getMapKeyObjectInspector();
                this.partialMapStrOI = (StringObjectInspector) partialMapOI.getMapValueObjectInspector();

            }
            /// The intermediate result is a map of hashes and strings,
            /// The final result is an array of strings
            if (m == Mode.FINAL || m == Mode.COMPLETE) {
                /// for final result
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            } else { /// m == Mode.PARTIAL1 || m == Mode.PARTIAL2
                return ObjectInspectorFactory.getStandardMapObjectInspector(
                        PrimitiveObjectInspectorFactory.javaLongObjectInspector,
                        PrimitiveObjectInspectorFactory.javaStringObjectInspector
                );
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            SketchSetBuffer buff = new SketchSetBuffer();
            buff.init(sketchSetSize);
            return buff;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            Object strObj = parameters[0];

            if (strObj != null) {
                String str = inputStrOI.getPrimitiveJavaObject(strObj);
                SketchSetBuffer myagg = (SketchSetBuffer) agg;
                myagg.addItem(str);

            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            /// Partial is going to be a map of strings and hashes
            SketchSetBuffer myagg = (SketchSetBuffer) agg;

            if (partial != null) {
                Map<Object, Object> partialResult = (Map<Object, Object>) this.partialMapOI.getMap(partial);
                if (partialResult != null) {
                    //// Place SKETCH_SIZE into the partial map ...
                    if (myagg.getSize() == -1) {
                        for (Map.Entry entry : partialResult.entrySet()) {
                            Long hash = this.partialMapHashOI.get(entry.getKey());
                            String item = partialMapStrOI.getPrimitiveJavaObject(entry.getValue());
                            if (item.equals(SKETCH_SIZE_STR)) {
                                this.sketchSetSize = (int) hash.intValue();
                                myagg.init(sketchSetSize);
                                break;
                            }
                        }
                    }
                    for (Map.Entry entry : partialResult.entrySet()) {
                        Long hash = this.partialMapHashOI.get(entry.getKey());
                        String item = partialMapStrOI.getPrimitiveJavaObject(entry.getValue());
                        if (!item.equals(SKETCH_SIZE_STR)) {
                            myagg.addHash(hash, item);
                        }
                    }
                }
            }
        }

        @Override
        public void reset(AggregationBuffer buff) throws HiveException {
            SketchSetBuffer sketchBuff = (SketchSetBuffer) buff;
            sketchBuff.reset();
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            SketchSetBuffer myagg = (SketchSetBuffer) agg;
            return myagg.getSketchItems();
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            SketchSetBuffer myagg = (SketchSetBuffer) agg;
            return myagg.getPartialMap();
        }
    }


}
