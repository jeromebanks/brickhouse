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


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;


/**
 * Truncate an array, and only return the first N elements
 */
public class TruncateArrayUDF extends GenericUDF {
    private ListObjectInspector listInspector;
    private StandardListObjectInspector returnInspector;
    private IntObjectInspector intInspector;


    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        int numVals = intInspector.get(arg0[1].get());
        Object uninspListObj = arg0[0].get();
        int listSize = listInspector.getListLength(uninspListObj);

        Object truncatedListObj = returnInspector.create(numVals);
        for (int i = 0; i < numVals && i < listSize; ++i) {
            returnInspector.set(truncatedListObj, i, listInspector.getListElement(uninspListObj, i));
        }
        return truncatedListObj;
    }


    @Override
    public String getDisplayString(String[] arg0) {
        return "truncate_array(" + arg0[0] + ", " + arg0[1] + " )";
    }


    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        ObjectInspector first = arg0[0];
        if (first.getCategory() == Category.LIST) {
            listInspector = (ListObjectInspector) first;
        } else {
            throw new UDFArgumentException(" Expecting an array and an int as arguments ");
        }

        ObjectInspector second = arg0[1];
        if (second.getCategory() == Category.PRIMITIVE) {
            PrimitiveObjectInspector secondPrim = (PrimitiveObjectInspector) second;
            if (secondPrim.getPrimitiveCategory() == PrimitiveCategory.INT) {
                intInspector = (IntObjectInspector) second;
            } else {
                throw new UDFArgumentException(" Expecting an array and an int as arguments ");
            }
        } else {
            throw new UDFArgumentException(" Expecting an array and an int as arguments ");
        }


        returnInspector = ObjectInspectorFactory.getStandardListObjectInspector(
                listInspector.getListElementObjectInspector());
        return returnInspector;
    }

}
