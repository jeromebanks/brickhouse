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
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;

/**
 * NumericRange is a UDTF for generating
 * a range of integers, useful for accessing array elements during
 * a lateral view.
 */
@Description(name = "numeric_range",
        value = "_FUNC_(a,b,c) - Generates a range of integers from a to b incremented by c"
                + " or the elements of a map into multiple rows and columns ")
public class NumericRange extends GenericUDTF {
    private IntObjectInspector startInspector = null;
    private IntObjectInspector endInspector = null;
    private IntObjectInspector incrementInspector = null;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs)
            throws UDFArgumentException {
        if (argOIs.length == 0 || argOIs.length > 3) {
            throw new UDFArgumentException("NumericRange takes 1 to 3 integer arguments");
        }
        for (ObjectInspector oi : argOIs) {
            if (oi.getCategory() != Category.PRIMITIVE) {
                throw new UDFArgumentException("NumericRange takes 1 to 3 integer arguments");
            }
        }
        if (argOIs.length == 1) {
            endInspector = (IntObjectInspector) argOIs[0];
        } else if (argOIs.length == 2) {
            startInspector = (IntObjectInspector) argOIs[0];
            endInspector = (IntObjectInspector) argOIs[1];
        } else if (argOIs.length == 3) {
            startInspector = (IntObjectInspector) argOIs[0];
            endInspector = (IntObjectInspector) argOIs[1];
            incrementInspector = (IntObjectInspector) argOIs[2];
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        fieldNames.add("n");
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                fieldOIs);
    }

    private final Object[] forwardListObj = new Object[1];

    @Override
    public void process(Object[] args) throws HiveException {
        int start = 0;
        int nd = 0;
        int incr = 1;
        switch (args.length) {
            case 1:
                nd = endInspector.get(args[0]);
                break;
            case 2:
                start = startInspector.get(args[0]);
                nd = endInspector.get(args[1]);
                break;
            case 3:
                start = startInspector.get(args[0]);
                nd = endInspector.get(args[1]);
                incr = incrementInspector.get(args[2]);
                break;
        }

        for (int i = start; i < nd; i += incr) {
            forwardListObj[0] = new Integer(i);
            this.forward(forwardListObj);
        }
    }

    @Override
    public void close() throws HiveException {

    }

}
