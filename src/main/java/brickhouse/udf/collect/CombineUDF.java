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
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * UDF for combining two lists or two maps together.
 */

@Description(name = "combine",
        value = "_FUNC_(a,b) - Returns a combined list of two lists, or a combined map of two maps "
)
public class CombineUDF extends GenericUDF {
    private Category category;
    private StandardListObjectInspector stdListInspector;
    private StandardMapObjectInspector stdMapInspector;
    private ListObjectInspector[] listInspectorList;
    private MapObjectInspector[] mapInspectorList;

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        if (category == Category.LIST) {

            int currSize = 0;
            Object theList = stdListInspector.create(currSize);
            int lastIdx = 0;
            for (int i = 0; i < args.length; ++i) {
                List addList = listInspectorList[i].getList(args[i].get());
                currSize += addList.size();
                stdListInspector.resize(theList, currSize);

                for (int j = 0; j < addList.size(); ++j) {
                    Object uninspObj = addList.get(j);
                    Object stdObj = ObjectInspectorUtils.copyToStandardObject(uninspObj, listInspectorList[i].getListElementObjectInspector(), ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
                    stdListInspector.set(theList, lastIdx + j, stdObj);
                }
                lastIdx += addList.size();
            }
            return theList;
        } else if (category == Category.MAP) {
            Object theMap = stdMapInspector.create();
            for (int i = 0; i < args.length; ++i) {
                if (args[i].get() != null) {
                    Map addMap = mapInspectorList[i].getMap(args[i].get());
                    for (Object uninspObj : addMap.entrySet()) {
                        Map.Entry uninspEntry = (Entry) uninspObj;
                        Object stdKey = ObjectInspectorUtils.copyToStandardObject(uninspEntry.getKey(), mapInspectorList[i].getMapKeyObjectInspector(), ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
                        Object stdVal = ObjectInspectorUtils.copyToStandardObject(uninspEntry.getValue(), mapInspectorList[i].getMapValueObjectInspector(), ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
                        stdMapInspector.put(theMap, stdKey, stdVal);
                    }
                }
            }
            return theMap;
        } else {
            throw new HiveException(" Only maps or lists are supported ");
        }
    }

    @Override
    public String getDisplayString(String[] args) {
        StringBuilder sb = new StringBuilder("combine( ");
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
        if (args.length < 2) {
            throw new UDFArgumentException("Usage: combine takes 2 or more maps or lists, and combines the result");
        }
        ObjectInspector first = args[0];
        this.category = first.getCategory();

        if (category == Category.LIST) {
            this.listInspectorList = new ListObjectInspector[args.length];
            this.listInspectorList[0] = (ListObjectInspector) first;
            for (int i = 1; i < args.length; ++i) {
                ObjectInspector argInsp = args[i];
                if (!ObjectInspectorUtils.compareTypes(first, argInsp)) {
                    throw new UDFArgumentException("Combine must either be all maps or all lists of the same type");
                }
                this.listInspectorList[i] = (ListObjectInspector) argInsp;
            }
            this.stdListInspector = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(first, ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
            return stdListInspector;
        } else if (category == Category.MAP) {
            this.mapInspectorList = new MapObjectInspector[args.length];
            this.mapInspectorList[0] = (MapObjectInspector) first;
            for (int i = 1; i < args.length; ++i) {
                ObjectInspector argInsp = args[i];
                if (!ObjectInspectorUtils.compareTypes(first, argInsp)) {
                    throw new UDFArgumentException("Combine must either be all maps or all lists of the same type");
                }
                this.mapInspectorList[i] = (MapObjectInspector) argInsp;
            }
            this.stdMapInspector = (StandardMapObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(first, ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
            return stdMapInspector;
        } else {
            throw new UDFArgumentException(" combine only takes maps or lists.");
        }

    }

}
