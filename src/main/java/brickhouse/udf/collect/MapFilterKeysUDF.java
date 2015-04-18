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
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Return a  map of entries from a map, for a given set of keys.
 *
 * @author jeromebanks
 */

@Description(name = "map_filter_keys",
        value = "_FUNC_(map, key_array) - Returns the filtered entries of a map corresponding to a given set of keys "
)
public class MapFilterKeysUDF extends GenericUDF {
    private static final Logger LOG = Logger.getLogger(MapFilterKeysUDF.class);
    private MapObjectInspector mapInspector;
    private StandardMapObjectInspector retValInspector;
    private ListObjectInspector keyListInspector;


    private Map stdKeys(Map inspectMap) {
        Map objMap = new HashMap();
        for (Object inspKey : inspectMap.keySet()) {

            Object objKey = ((PrimitiveObjectInspector) mapInspector.getMapKeyObjectInspector()).getPrimitiveJavaObject(inspKey);
            objMap.put(objKey, inspKey);

        }
        return objMap;
    }


    private List inspectList(List inspectList) {
        List objList = new ArrayList();
        for (Object inspKey : inspectList) {

            Object objKey = ((PrimitiveObjectInspector) keyListInspector.getListElementObjectInspector()).getPrimitiveJavaObject(inspKey);

            objList.add(objKey);

        }
        return objList;
    }


    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        Map hiveMap = mapInspector.getMap(arg0[0].get());

        List keyValues = inspectList(keyListInspector.getList(arg0[1].get()));

        /// Convert all the keys to standard keys
        Map stdKeys = stdKeys(hiveMap);


        Map retVal = (Map) retValInspector.create();
        for (Object keyObj : keyValues) {
            if (stdKeys.containsKey(keyObj)) {
                Object hiveKey = stdKeys.get(keyObj);
                Object hiveVal = hiveMap.get(hiveKey);
                Object keyStd = ObjectInspectorUtils.copyToStandardObject(hiveKey, mapInspector.getMapKeyObjectInspector());
                Object valStd = ObjectInspectorUtils.copyToStandardObject(hiveVal, mapInspector.getMapValueObjectInspector());

                retVal.put(keyStd, valStd);
            }
        }
        return retVal;
    }


    @Override
    public String getDisplayString(String[] arg0) {
        return "map_filter_keys(" + arg0[0] + ", " + arg0[1] + " )";
    }


    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        ObjectInspector first = arg0[0];
        if (first.getCategory() == Category.MAP) {
            mapInspector = (MapObjectInspector) first;
        } else {
            throw new UDFArgumentException(" Expecting a map as first argument ");
        }

        ObjectInspector second = arg0[1];
        if (second.getCategory() == Category.LIST) {
            keyListInspector = (ListObjectInspector) second;
        } else {
            throw new UDFArgumentException(" Expecting a list as second argument ");
        }

        //// List inspector ...
        if (!(keyListInspector.getListElementObjectInspector().getCategory() == Category.PRIMITIVE)) {
            throw new UDFArgumentException(" Expecting a primitive as key list elements.");
        }
        ObjectInspector mapKeyInspector = mapInspector.getMapKeyObjectInspector();
        if (!(mapKeyInspector.getCategory() == Category.PRIMITIVE)) {
            throw new UDFArgumentException(" Expecting a primitive as map key elements.");
        }
        LOG.info(" Map has key type " + mapKeyInspector.getTypeName());
        LOG.info(" Key list has key type " + keyListInspector.getTypeName());
        if (((PrimitiveObjectInspector) keyListInspector.getListElementObjectInspector()).getPrimitiveCategory() != ((PrimitiveObjectInspector) mapKeyInspector).getPrimitiveCategory()) {
            throw new UDFArgumentException(" Expecting keys to be of same types.");
        }

        retValInspector = (StandardMapObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(first);
        return retValInspector;
    }

}
