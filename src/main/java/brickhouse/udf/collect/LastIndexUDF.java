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


/**
 * Workaround for the Hive bug
 * https://issues.apache.org/jira/browse/HIVE-1955
 * <p/>
 * FAILED: Error in semantic analysis: Line 4:3 Non-constant expressions for array indexes not supported key
 * <p/>
 * <p/>
 * Use instead of [ ] syntax,
 */
@Description(name = "last_index",
        value = "_FUNC_(x) - Last value in an array "
)
public class LastIndexUDF extends GenericUDF {
    private ListObjectInspector listInspector;


    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        Object list = arg0[0].get();
        int lastIdx = listInspector.getListLength(list) - 1;
        if (lastIdx >= 0) {
            Object unInsp = listInspector.getListElement(list, lastIdx);
            return unInsp;
        } else {
            return null;
        }
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "last_index( " + arg0[0] + " )";
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        if (arg0.length != 1) {
            throw new UDFArgumentException("last_index takes an array as an argument.");
        }
        if (arg0[0].getCategory() != Category.LIST) {
            throw new UDFArgumentException("last_index takes an array as an argument.");
        }
        listInspector = (ListObjectInspector) arg0[0];

        return listInspector.getListElementObjectInspector();
    }

}
