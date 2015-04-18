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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;

@Description(name = "array_flatten",
        value = "_FUNC_(array) - Returns the array with the elements flattened."
)
public class ArrayFlattenUDF extends GenericUDF {

    private ListObjectInspector listInspector;
    private StandardListObjectInspector returnInspector;
    private IntObjectInspector depthIntInspector;

    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        Object inputObject = arg0[0].get();

        if (arg0.length != 1 || inputObject == null) return null;

        //first, check if we are already flattened
        ObjectInspector elementInspector = listInspector.getListElementObjectInspector();
        if (elementInspector.getCategory() == Category.PRIMITIVE) return inputObject;

        //second, get the length of the resulting flat
        int inputLength = listInspector.getListLength(inputObject);
        int resultLength = 0;
        ListObjectInspector subArrayInspector = (ListObjectInspector) elementInspector;
        for (int i = 0; i < inputLength; i++) {
            resultLength += subArrayInspector.getListLength(listInspector.getListElement(inputObject, i));
        }


        //now, flatten the list by one level.
        Object flattenedListObj = returnInspector.create(resultLength);

        int resultIndex = 0;

        for (int i = 0; i < inputLength; i++) {
            Object element = listInspector.getListElement(inputObject, i);
            int subArrayLength = subArrayInspector.getListLength(element);
            for (int j = 0; j < subArrayLength; j++) {
                Object subArrayElement = subArrayInspector.getListElement(element, j);
                returnInspector.set(flattenedListObj, resultIndex, subArrayElement);
                ++resultIndex;
            }
        }

        return flattenedListObj;
    }

    @Override
    public String getDisplayString(String[] args) {
        String display = "array_flatten(" + args[0];
        if (args.length == 1) {
            display.concat(")");
        } else {
            display.concat(", " + args[1].toString() + ")");
        }
        return display;
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {

        if (arg0.length != 1 && arg0.length != 2) { // for now, array_flatten(array). Later array_flatten(array, depth)
            throw new UDFArgumentException("array_flatten : expected format array_flatten(array) or array_flatten(array, depth)");
        }

        ObjectInspector list = arg0[0];

        if (list.getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentException("array_flatten : expecting array as input, got " + list.getTypeName());
        }

        listInspector = (ListObjectInspector) list;

        if (arg0.length == 2) { // should I check if this is less than 0?
            ObjectInspector depth = arg0[1];
            PrimitiveObjectInspector depthInsp = (PrimitiveObjectInspector) depth;
            if (depthInsp.getPrimitiveCategory() == PrimitiveCategory.INT) {
                depthIntInspector = (IntObjectInspector) depth;
            } else {
                throw new UDFArgumentException("array_flatten : expecting optional second parameter as INT");
            }
        }

        if (listInspector.getListElementObjectInspector().getCategory() == Category.PRIMITIVE) {
            returnInspector = ObjectInspectorFactory.getStandardListObjectInspector(
                    listInspector.getListElementObjectInspector());
        } else {
            ListObjectInspector subArrayInspector = (ListObjectInspector) listInspector.getListElementObjectInspector();
            ;
            returnInspector = ObjectInspectorFactory.getStandardListObjectInspector(
                    subArrayInspector.getListElementObjectInspector());
        }
        return returnInspector;
    }
}