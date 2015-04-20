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

import brickhouse.analytics.uniques.SketchSet;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.List;


/**
 * XXX Snarfed from multiday counter ...
 * TODO write one UDF which can be configured to sketch or count
 * TODO Generalize to represent other periods besides Days
 * TODO
 * XXX Probably needs Const object inspectors
 * <p/>
 * Count and count uniques for several day periods
 * ( i.e produce 1, 7 and 30 counts for various events)
 * <p>Input is a YYYYMMDD representation of the date counts are being generated,
 * a date representation of the date associated with the events,
 * a bigint of the event count for that day period,
 * an array of uniques for that count (or a sketch set for those uniques),
 * and an array of ints representing the dates being counted over ( ie. [1,7,30] ).
 * </p>
 * <p/>
 * <p>Output is a array of structs containing the num of days counted, the sum of events
 * over that date
 */


@Description(name = "multiday_sketch",
        value = "_FUNC_(x) - Returns a count of events over several different periods,"
)
public class MultiDaySketcherUDAF extends AbstractGenericUDAFResolver {
    private static final Logger LOG = Logger.getLogger(MultiDaySketcherUDAF.class);
    private static final String SKETCH_FLAG_PROP = "klout.warehouse.multiday_sketch";


    public MultiDaySketcherUDAF() {

    }

    /**
     * Parameters are event date, event count, event uniques, asof date, period array ,
     */
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        for (int i = 0; i < parameters.length; ++i) {
            LOG.info("Type " + i + " == " + parameters[i].getTypeName() + " category " + parameters[i].getCategory().name());
        }
        if (parameters.length != 5 && parameters.length != 6) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "multiday_sketch takes date, count, array, date, array ");
        }
        if (parameters[0].getCategory() != Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "multiday_sketch takes date, count, array, date, array ");

        }

        MultiDayAggUDAFEvaluator mdEval = new MultiDayAggUDAFEvaluator();
        return mdEval;
    }

    public static class MultiDayAggUDAFEvaluator extends GenericUDAFEvaluator {
        private static DateTimeFormatter yyyymmdd = DateTimeFormat.forPattern("yyyyMMdd");
        private Integer[] daysArr;
        private DateTime asofDate;
        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
        // of objs)
        private StandardListObjectInspector internalMergeOI;

        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
        private StringObjectInspector asofInspector;
        private StringObjectInspector dtInspector;
        private LongObjectInspector longInspector;
        private ListObjectInspector uniqInspector;
        private ListObjectInspector daysArrInspector;


        static class MultiDaySketchBuffer implements AggregationBuffer {
            long counts[];
            SketchSet[] sketches;
        }


        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);

            LOG.info(" MODE = " + m.name() + " Num parameters = " + parameters.length);
            for (int i = 0; i < parameters.length; ++i) {
                LOG.info(" Parameter [ " + i + " ] == " + parameters[i]);
            }

            if (m.equals(Mode.PARTIAL1) || m.equals(Mode.COMPLETE)) {

                Object firstParam = parameters[0];
                if (firstParam instanceof StringObjectInspector) {
                    dtInspector = (StringObjectInspector) parameters[0];
                    longInspector = (LongObjectInspector) parameters[1];
                    uniqInspector = (ListObjectInspector) parameters[2];
                    asofInspector = (StringObjectInspector) parameters[3];
                    daysArrInspector = (ListObjectInspector) parameters[4];
                }

                //// return a list of list of strings ...
                //// First string will the the count, rest are the uniques ...
                ListObjectInspector strListInspector = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
                ListObjectInspector listInspector = ObjectInspectorFactory.getStandardListObjectInspector(strListInspector);

                return listInspector;


                ///} else if( m.equals( Mode.FINAL) || m.equals( Mode.PARTIAL2)) {
            } else {
                this.internalMergeOI = (StandardListObjectInspector) parameters[0];


                List<String> fieldNames = new ArrayList<String>();
                List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>();
                fieldNames.add("num_days");
                fieldInspectors.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
                fieldNames.add("cnt");
                fieldInspectors.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
                fieldNames.add("sketch_sets");

                fieldInspectors.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector));

                ObjectInspector structType = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldInspectors);

                ObjectInspector retType = ObjectInspectorFactory.getStandardListObjectInspector(structType);

                return retType;
            }
        }

        private void addMultiDay(MultiDaySketchBuffer mdCounter, DateTime dt, Long cnt, List<Object> uniqs) {
            for (int i = 0; i < daysArr.length; ++i) {
                int daysBetween = Days.daysBetween(dt, asofDate).getDays();
                ///LOG.info( " DT = "+ dt + " asofDate = " + asofDate +  " daysBetween = " + daysBetween);
                if (daysBetween < (Integer) daysArr[i]) {
                    mdCounter.counts[i] += cnt;
                    ///LOG.info( "Days between = " + daysBetween + " for idx "+ i + " with val " + daysArr[i] +  " cnt = " + mdCounter.counts[i] );
                    for (Object unObj : uniqs) {
                        String uniqStr = ((StringObjectInspector) uniqInspector.getListElementObjectInspector()).getPrimitiveJavaObject(unObj);
                        ///LOG.info( "   Adding Unique str " + uniqStr);
                        mdCounter.sketches[i].addItem(uniqStr);
                    }
                }
            }
        }


        private void setDaysArr(Object obj) {
            List inspected = this.daysArrInspector.getList(obj);
            daysArr = new Integer[inspected.size()];
            int idx = 0;
            for (Object elem : inspected) {
                daysArr[idx++] = (Integer) ((IntObjectInspector) daysArrInspector.getListElementObjectInspector()).getPrimitiveJavaObject(elem);
            }
        }

        private void setAsofDate(Object obj) {
            String str = asofInspector.getPrimitiveJavaObject(obj);
            asofDate = getDateTime(str);
        }

        private DateTime getDateTime(String str) {
            DateTime dt = yyyymmdd.parseDateTime(str);
            return dt;
        }

        private long getLong(Object obj) {
            return longInspector.get(obj);
        }

        private List getList(Object obj) {
            return this.uniqInspector.getList(obj);
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AggregationBuffer buff = new MultiDaySketchBuffer();
            reset(buff);
            return buff;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            if (daysArr == null) {
                setDaysArr(parameters[4]);
                reset(agg);
            }
            if (asofDate == null) {
                setAsofDate(parameters[3]);
            }

            MultiDaySketchBuffer myagg = (MultiDaySketchBuffer) agg;
            DateTime dt = getDateTime(dtInspector.getPrimitiveJavaObject(parameters[0]));
            long cnt = getLong(parameters[1]);
            List<Object> uniqList = getList(parameters[2]);
            addMultiDay(myagg, dt, cnt, uniqList);
        }


        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            ////LOG.info(" MERGE IS CALLED partial is " + partial + " AGG is "  + agg);
            List partialResultList = internalMergeOI.getList(partial);
            if (daysArr == null) {
                daysArr = new Integer[partialResultList.size()];
            }
            MultiDaySketchBuffer myagg = (MultiDaySketchBuffer) agg;
            if (myagg.counts == null) {
                reset(myagg);
            }

            ListObjectInspector subListInspector = (ListObjectInspector) internalMergeOI.getListElementObjectInspector();
            StringObjectInspector strInspector = (StringObjectInspector) subListInspector.getListElementObjectInspector();
            int idx = 0;
            for (Object strListObj : partialResultList) {
                List strList = subListInspector.getList(strListObj);

                String numDaysStr = strInspector.getPrimitiveJavaObject(strList.get(0));
                daysArr[idx] = Integer.decode(numDaysStr);
                ///LOG.info(" numDays =  " + numDaysStr);

                String cntStr = strInspector.getPrimitiveJavaObject(strList.get(1));
                ///LOG.info(" Count Strr = " + cntStr);
                Long cnt = Long.decode(cntStr);
                myagg.counts[idx] += cnt;
                for (int j = 2; j < strList.size(); ++j) {
                    String uniqStr = strInspector.getPrimitiveJavaObject(strList.get(j));
                    myagg.sketches[idx].addItem(uniqStr);
                }
                idx++;
            }
        }

        @Override
        public void reset(AggregationBuffer buff) throws HiveException {
            MultiDaySketchBuffer countBuff = (MultiDaySketchBuffer) buff;
            if (daysArr != null) {
                countBuff.counts = new long[daysArr.length];
                countBuff.sketches = new SketchSet[daysArr.length];
                for (int i = 0; i < countBuff.sketches.length; ++i)
                    countBuff.sketches[i] = new SketchSet();
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            ////LOG.info( "Terminate  " + agg);
            MultiDaySketchBuffer myagg = (MultiDaySketchBuffer) agg;
            List<List> ret = new ArrayList<List>();
            for (int i = 0; i < daysArr.length; ++i) {
                ArrayList structArr = new ArrayList();
                structArr.add(daysArr[i]); /// num_days
                structArr.add(myagg.counts[i]);

                List<String> sketchList = myagg.sketches[i].getMinHashItems();
                structArr.add(sketchList);


                ret.add(structArr);
            }
            return ret;

        }


        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            ///LOG.info( "Terminate partial " + agg);
            MultiDaySketchBuffer myagg = (MultiDaySketchBuffer) agg;
            List<List> ret = new ArrayList<List>();
            for (int i = 0; i < daysArr.length; ++i) {
                ArrayList strList = new ArrayList();

                strList.add(Integer.toString(daysArr[i]));
                strList.add(Long.toString(myagg.counts[i]));


                List<String> itemList = myagg.sketches[i].getMinHashItems();
                for (String minHashItem : itemList) {
                    strList.add(minHashItem); //// XXX TODO for sketch sets, pass the hash as well ...
                }
                ret.add(strList);
            }
            return ret;
        }
    }

}
