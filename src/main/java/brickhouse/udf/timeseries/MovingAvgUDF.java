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


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Moving average of a timeseries array of data
 */
@Description(
        name = "moving_avg",
        value = " return the moving average of a time series for a given timewindow"
)
public class MovingAvgUDF extends GenericUDF {
    private static final Logger LOG = Logger.getLogger(MovingAvgUDF.class);
    private ListObjectInspector listInspector;
    private IntObjectInspector dayWindowInspector;

    private List<Double> parseDoubleList(List<Object> objList) {
        List<Double> arrList = new ArrayList<Double>();
        for (Object obj : objList) {

            Object dblObj = ((PrimitiveObjectInspector) (listInspector.getListElementObjectInspector())).getPrimitiveJavaObject(obj);
            if (dblObj instanceof Number) {
                Number dblNum = (Number) dblObj;
                arrList.add(dblNum.doubleValue());
            } else {
                //// Try to coerce it otherwise
                String dblStr = (dblObj.toString());
                try {
                    Double dblCoerce = Double.parseDouble(dblStr);
                    arrList.add(dblCoerce);
                } catch (NumberFormatException formatExc) {
                    LOG.info(" Unable to interpret " + dblStr + " as a number");
                }
            }

        }
        return arrList;
    }

    public List<Double> evaluate(List<Object> timeSeriesObj, int dayWindow) {

        List<Double> timeSeries = this.parseDoubleList(timeSeriesObj);

        List<Double> mvnAvgTimeSeries = new ArrayList<Double>(timeSeries.size());
        double mvnTotal = 0.0;

        for (int i = 0; i < timeSeries.size(); ++i) {
            mvnTotal += timeSeries.get(i); //// Do we want to set days befo
            if (i >= dayWindow) {
                mvnTotal -= timeSeries.get(i - dayWindow);
                double mvnAvg = mvnTotal / ((double) dayWindow);
                mvnAvgTimeSeries.add(mvnAvg);
            } else {
                if (i > 0) {  ///smooth out according to number of days for less than day window
                    double mvnAvg = mvnTotal / ((double) i + 1.0);
                    mvnAvgTimeSeries.add(mvnAvg);
                } else {
                    mvnAvgTimeSeries.add(mvnTotal); ///
                }
            }
        }
        return mvnAvgTimeSeries;
    }

    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        List argList = listInspector.getList(arg0[0].get());
        int dayWindow = dayWindowInspector.get(arg0[1].get());
        if (argList != null)
            return evaluate(argList, dayWindow);
        else
            return null;
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "moving_avg(" + arg0[0] + ")";
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        this.listInspector = (ListObjectInspector) arg0[0];
        this.dayWindowInspector = (IntObjectInspector) arg0[1];
        LOG.info("Moving Average Object input type is " + listInspector + " element = " + listInspector.getListElementObjectInspector());

        return ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    }
}
