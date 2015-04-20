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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * UDF for the set difference of two arrays or maps.
 */

@Description(name = "set_diff",
        value = "_FUNC_(a,b) - Returns a list of those items in a, but not in b "
)
public class SetDifferenceUDF extends GenericUDF {
    private Category category;
    private ListObjectInspector list1Inspector;
    private ListObjectInspector list2Inspector;
    private MapObjectInspector map1Inspector;
    private MapObjectInspector map2Inspector;
    private PrimitiveObjectInspector prim1Inspector;
    private PrimitiveObjectInspector prim2Inspector;
    private StandardListObjectInspector stdListInspector;
    private StandardMapObjectInspector stdMapInspector;

    public List evaluate(List l1, List l2) {
        if (l1 == null) {
            return null;
        }
        //// Use a HashSet to avoid linear lookups , for large lists
        HashSet negSet = new HashSet();
        if (l2 != null) {
            for (Object lObj : l2) {
                Object inspObj = prim2Inspector.getPrimitiveJavaObject(lObj);
                negSet.add(inspObj);
            }
        }
        List newList = (List) stdListInspector.create(0);
        for (Object obj : l1) {
            Object inspObj = prim1Inspector.getPrimitiveJavaObject(obj);
            if (!negSet.contains(inspObj)) {
                newList.add(inspObj);
            }
        }
        return newList;
    }

    public Map evaluate(Map m1, Map m2) {
        Map newMap = (Map) stdMapInspector.create();
        if (m1 == null) {
            return null;
        }
        HashSet negSet = new HashSet();
        if (m2 != null) {
            for (Object mObj : m2.keySet()) {
                Object inspObj = prim2Inspector.getPrimitiveJavaObject(mObj);
                negSet.add(inspObj);
            }
        }

        if (m1.size() > 0)
            for (Object k : m1.keySet()) {
                Object inspObj = prim1Inspector.getPrimitiveJavaObject(k);
                if (!negSet.contains(inspObj)) {
                    Object valObj = m1.get(k);
                    Object stdVal = ObjectInspectorUtils.copyToStandardObject(valObj, map1Inspector.getMapValueObjectInspector());
                    newMap.put(inspObj, stdVal);
                }
            }
        return newMap;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        if (category == Category.LIST) {
            List theList1 = list1Inspector.getList(args[0].get());
            List theList2 = list2Inspector.getList(args[1].get());
            List retList = evaluate(theList1, theList2);
            return retList;
        } else if (category == Category.MAP) {
            Map theMap1 = map1Inspector.getMap(args[0].get());
            Map theMap2 = map2Inspector.getMap(args[1].get());
            Map retMap = evaluate(theMap1, theMap2);
            return retMap;
        } else {
            throw new HiveException(" Only maps or lists are supported ");
        }
    }

    @Override
    public String getDisplayString(String[] args) {
        StringBuilder sb = new StringBuilder("set_diff( ");
        for (int i = 0; i < args.length - 1; ++i) {
            sb.append(args[i]);
            sb.append(",");
        }
        sb.append(args[args.length - 1]);
        sb.append(")");
        return sb.toString();
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] args)
            throws UDFArgumentException {
        if (args.length != 2) {
            throw new UDFArgumentException("Usage: set_diff takes 2  maps or lists, and returns the difference");
        }
        ObjectInspector first = args[0];
        ObjectInspector second = args[1];

        if (first.getCategory() == Category.LIST
                && second.getCategory() == Category.LIST) {
            category = first.getCategory();
            list1Inspector = (ListObjectInspector) first;
            list2Inspector = (ListObjectInspector) second;

            if (list1Inspector.getListElementObjectInspector().getCategory() != Category.PRIMITIVE
                    || list2Inspector.getListElementObjectInspector().getCategory() != Category.PRIMITIVE) {
                throw new UDFArgumentException(" set_diff only takes maps or lists of primitives.");
            }
            prim1Inspector = (PrimitiveObjectInspector) list1Inspector.getListElementObjectInspector();
            prim2Inspector = (PrimitiveObjectInspector) list2Inspector.getListElementObjectInspector();
            if (prim1Inspector.getPrimitiveCategory() != prim2Inspector.getPrimitiveCategory()) {
                throw new UDFArgumentException(" set_diff takes only lists of the same primitive type.");
            }

            stdListInspector = ObjectInspectorFactory.getStandardListObjectInspector(
                    ObjectInspectorUtils.getStandardObjectInspector(prim1Inspector, ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA));
            return stdListInspector;
        } else if (first.getCategory() == Category.MAP
                && second.getCategory() == Category.MAP) {
            category = first.getCategory();
            map1Inspector = (MapObjectInspector) first;
            map2Inspector = (MapObjectInspector) second;

            if (map1Inspector.getMapKeyObjectInspector().getCategory() != Category.PRIMITIVE
                    || map2Inspector.getMapKeyObjectInspector().getCategory() != Category.PRIMITIVE) {
                throw new UDFArgumentException(" set_diff only takes maps or lists of primitives.");
            }
            prim1Inspector = (PrimitiveObjectInspector) map1Inspector.getMapKeyObjectInspector();
            prim2Inspector = (PrimitiveObjectInspector) map2Inspector.getMapKeyObjectInspector();
            if (prim1Inspector.getPrimitiveCategory() != prim2Inspector.getPrimitiveCategory()) {
                throw new UDFArgumentException(" set_diff takes only maps of the same primitive type.");
            }

            stdMapInspector = ObjectInspectorFactory.getStandardMapObjectInspector(
                    ObjectInspectorUtils.getStandardObjectInspector(prim1Inspector, ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA),
                    ObjectInspectorUtils.getStandardObjectInspector(map1Inspector.getMapValueObjectInspector())
            );
            return stdMapInspector;
        } else {
            throw new UDFArgumentException(" set_diff only takes maps or lists.");
        }
    }

}
