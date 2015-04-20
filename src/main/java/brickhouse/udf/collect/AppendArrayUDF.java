package brickhouse.udf.collect;
/**
 * Copyright 2012 Klout, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import java.util.List;

/**
 * Append an object to the end of an Array
 */
public class AppendArrayUDF extends GenericUDF {
    private ListObjectInspector listInspector;
    private PrimitiveObjectInspector listElemInspector;
    private boolean returnWritables;
    private PrimitiveObjectInspector primInspector;

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        List objList = listInspector.getList(args[0].get());
        Object objToAppend = args[1].get();
        Object[] res = new Object[objList.size() + 1];
        for (int i = 0; i < objList.size(); i++) {
            Object o = objList.get(i);
            res[i] = returnWritables ?
                    listElemInspector.getPrimitiveWritableObject(o) :
                    listElemInspector.getPrimitiveJavaObject(o);
        }
        res[res.length - 1] = returnWritables ?
                primInspector.getPrimitiveWritableObject(objToAppend) :
                primInspector.getPrimitiveJavaObject(objToAppend);
        return res;
    }

    @Override
    public String getDisplayString(String[] args) {
        return "append_array(" + args[0] + ", " + args[1] + ")";
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] params)
            throws UDFArgumentException {
        try {
            listInspector = (ListObjectInspector) params[0];
            listElemInspector = (PrimitiveObjectInspector) listInspector.getListElementObjectInspector();
            primInspector = (PrimitiveObjectInspector) params[1];
            if (listElemInspector.getPrimitiveCategory() != primInspector.getPrimitiveCategory()) {
                throw new UDFArgumentException(
                        "append_array expects the list type to match the type of the value being appended");
            }
            returnWritables = listElemInspector.preferWritable();
            return ObjectInspectorFactory.getStandardListObjectInspector(
                    ObjectInspectorUtils.getStandardObjectInspector(listElemInspector));
        } catch (ClassCastException e) {
            throw new UDFArgumentException("append_array expects a list as the first argument and a primitive " +
                    "as the second argument and the list type to match the type of the value being appended");
        }
    }
}
