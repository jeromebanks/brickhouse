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
 * Multiply a vector of numbers times a scalar value
 */
@Description(
        name = "vector_scalar_mult",
        value = " Multiply a vector times a scalar"
)
public class VectorMultUDF extends GenericUDF {
    private static final Logger LOG = Logger.getLogger(VectorMultUDF.class);
    private ListObjectInspector listInspector;
    private MapObjectInspector mapInspector;
    private PrimitiveObjectInspector valueInspector;
    private PrimitiveObjectInspector scalarInspector;

    private StandardListObjectInspector retListInspector;
    private StandardMapObjectInspector retMapInspector;


    public Object evaluateList(Object listObj, double scalar) {
        Object retList = retListInspector.create(0);
        for (int i = 0; i < listInspector.getListLength(listObj); ++i) {
            Object listVal = this.listInspector.getListElement(listObj, i);
            double listDbl = NumericUtil.getNumericValue(valueInspector, listVal);
            double newVal = listDbl * scalar;
            retListInspector.set(retList, i, NumericUtil.castToPrimitiveNumeric(newVal,
                    ((PrimitiveObjectInspector) retListInspector.getListElementObjectInspector()).getPrimitiveCategory()));
        }
        return retList;
    }

    public Object evaluateMap(Object uninspMapObj, double scalar) {
        Object retMap = retMapInspector.create();
        Map map = mapInspector.getMap(uninspMapObj);
        for (Object mapKey : map.keySet()) {
            Object mapValObj = map.get(mapKey);
            double mapValDbl = NumericUtil.getNumericValue(valueInspector, mapValObj);

            double newVal = mapValDbl * scalar;
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
        double dbl = NumericUtil.getNumericValue(scalarInspector, arg0[1].get());
        if (listInspector != null) {
            return evaluateList(arg0[0].get(), dbl);
        } else {
            return evaluateMap(arg0[0].get(), dbl);
        }
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "vector_scalar_mult";
    }


    private void usage(String message) throws UDFArgumentException {
        LOG.error("vector_scalar_mult: Multiply a vector times a scalar value : " + message);
        throw new UDFArgumentException("vector_scalar_mult: Multiply a vector times a scalar value : " + message);
    }


    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        if (arg0.length != 2)
            usage("Must have two arguments.");

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
        if (arg0[1].getCategory() != Category.PRIMITIVE) {
            usage(" scalar needs to be a primitive type.");
        }
        this.scalarInspector = (PrimitiveObjectInspector) arg0[1];
        if (!NumericUtil.isNumericCategory(scalarInspector.getPrimitiveCategory())) {
            usage(" Scalar needs to be a numeric type");
        }


        if (listInspector != null) {
            retListInspector = ObjectInspectorFactory.getStandardListObjectInspector(
                    ObjectInspectorUtils.getStandardObjectInspector(valueInspector,
                            ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA));
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
