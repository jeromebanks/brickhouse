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
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * Normalize a Vector
 */
@Description(
        name = "vector_normalize",
        value = " Normalize a Vector"
)
public class VectorNormalizeUDF extends GenericUDF {
    private static final Logger LOG = Logger.getLogger(VectorNormalizeUDF.class);
    private ListObjectInspector listInspector;
    private MapObjectInspector mapInspector;
    private PrimitiveObjectInspector valueInspector;

    private StandardListObjectInspector retListInspector;
    private StandardMapObjectInspector retMapInspector;


    public Object evaluateList(Object listObj) {
        Object retList = retListInspector.create(0);
        double tot = 0.0;
        for (int i = 0; i < listInspector.getListLength(listObj); ++i) {
            Object listVal = this.listInspector.getListElement(listObj, i);
            double listDbl = NumericUtil.getNumericValue(valueInspector, listVal);
            tot += listDbl;
        }
        for (int i = 0; i < listInspector.getListLength(listObj); ++i) {
            Object listVal = this.listInspector.getListElement(listObj, i);
            double listDbl = NumericUtil.getNumericValue(valueInspector, listVal);
            retListInspector.set(retList, i, NumericUtil.castToPrimitiveNumeric(listDbl / tot,
                    ((PrimitiveObjectInspector) retListInspector.getListElementObjectInspector()).getPrimitiveCategory()));
        }

        return retList;
    }

    public Object evaluateMap(Object uninspMapObj) {
        Object retMap = retMapInspector.create();
        Map map = mapInspector.getMap(uninspMapObj);
        double tot = 0.0;
        for (Object mapKey : map.keySet()) {
            Object mapValObj = map.get(mapKey);
            double mapValDbl = NumericUtil.getNumericValue(valueInspector, mapValObj);
            tot += mapValDbl;
        }
        for (Object mapKey : map.keySet()) {
            Object mapValObj = map.get(mapKey);
            double mapValDbl = NumericUtil.getNumericValue(valueInspector, mapValObj);
            double newVal = mapValDbl / tot;

            Object stdKey = ObjectInspectorUtils.copyToStandardJavaObject(mapKey,
                    mapInspector.getMapKeyObjectInspector());
            Object stdVal = NumericUtil.castToPrimitiveNumeric(newVal,
                    ((PrimitiveObjectInspector) retMapInspector.getMapValueObjectInspector()).getPrimitiveCategory());
            retMapInspector.put(retMap, stdKey, stdVal);

        }
        return retMap;
    }


    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        if (listInspector != null) {
            return evaluateList(arg0[0].get());
        } else {
            return evaluateMap(arg0[0].get());
        }
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "vector_normalize";
    }


    private void usage(String message) throws UDFArgumentException {
        LOG.error("vector_normalize: Normalize a vector : " + message);
        throw new UDFArgumentException("vector_normalize: Normalize a vector : " + message);
    }


    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        if (arg0.length != 1)
            usage("Must have one argument.");

        if (arg0[0].getCategory() == Category.MAP) {
            this.mapInspector = (MapObjectInspector) arg0[0];

            if (mapInspector.getMapKeyObjectInspector().getCategory() != Category.PRIMITIVE)
                usage("Vector map key must be a primitive");

            if (mapInspector.getMapValueObjectInspector().getCategory() != Category.PRIMITIVE)
                usage("Vector map value must be a primitive");

            this.valueInspector = (PrimitiveObjectInspector) mapInspector.getMapValueObjectInspector();
        } else if (arg0[0].getCategory() == Category.LIST) {
            this.listInspector = (ListObjectInspector) arg0[0];

            if (listInspector.getListElementObjectInspector().getCategory() != Category.PRIMITIVE)
                usage("Vector array value must be a primitive");

            this.valueInspector = (PrimitiveObjectInspector) listInspector.getListElementObjectInspector();
        } else {
            usage("First argument must be an array or map");
        }

        if (!NumericUtil.isNumericCategory(valueInspector.getPrimitiveCategory())) {
            usage(" Vector values must be numeric");
        }

        if (listInspector != null) {
            retListInspector = ObjectInspectorFactory.getStandardListObjectInspector(
                    ObjectInspectorUtils.getStandardObjectInspector(valueInspector));
            return retListInspector;
        } else {
            retMapInspector = ObjectInspectorFactory.getStandardMapObjectInspector(
                    ObjectInspectorUtils.getStandardObjectInspector(mapInspector.getMapKeyObjectInspector(),
                            ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA),
                    ObjectInspectorUtils.getStandardObjectInspector(valueInspector,
                            ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA));
            return retMapInspector;
        }
    }
}
