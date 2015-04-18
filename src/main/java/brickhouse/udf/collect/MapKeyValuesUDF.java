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
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Given a Map, return an Array of structs
 * containing key and value
 */

@Description(name = "map_key_values",
        value = "_FUNC_(map) - Returns a Array of key-value pairs contained in a Map"
)
public class MapKeyValuesUDF extends GenericUDF {
    private MapObjectInspector moi;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentException("Usage : map_key_values( map) ");
        }
        if (!arguments[0].getCategory().equals(Category.MAP)) {
            throw new UDFArgumentException("Usage : map_key_values( map) ");
        }

        moi = (MapObjectInspector) arguments[0];

        ////
        List<String> structFieldNames = new ArrayList<String>();
        List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
        structFieldNames.add("key");
        structFieldObjectInspectors.add(moi.getMapKeyObjectInspector());
        structFieldNames.add("value");
        structFieldObjectInspectors.add(moi.getMapValueObjectInspector());

        ObjectInspector keyOI = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
        ObjectInspector arrayOI = ObjectInspectorFactory.getStandardListObjectInspector(keyOI);

        return arrayOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Map<?, ?> map = moi.getMap(arguments[0].get());
        Object[] res = new Object[map.size()];
        int i = 0;
        for (Map.Entry e : map.entrySet()) {
            res[i++] = new Object[]{e.getKey(), e.getValue()};
        }
        return res;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "map_key_values( " + children[0] + " )";
    }

}
