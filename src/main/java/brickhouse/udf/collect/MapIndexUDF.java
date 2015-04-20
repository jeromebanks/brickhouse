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
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.log4j.Logger;

import java.util.Map;


/**
 * Workaround for the Hive bug
 * https://issues.apache.org/jira/browse/HIVE-1955
 * <p/>
 * FAILED: Error in semantic analysis: Line 4:3 Non-constant expressions for array indexes not supported key
 * <p/>
 * <p/>
 * Use instead of [ ] syntax,
 */
public class MapIndexUDF extends GenericUDF {
    private static final Logger LOG = Logger.getLogger(MapIndexUDF.class);
    private PrimitiveObjectInspector keyInspector;
    private MapObjectInspector mapInspector;
    private PrimitiveObjectInspector mapKeyInspector;
    private CreateWithPrimitive createKey;

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        Map<?, ?> map = mapInspector.getMap(args[0].get());
        Object key = keyInspector.getPrimitiveJavaObject(args[1].get());
        if (key == null) {
            return map.get(null);
        }
        if (createKey != null) {
            return map.get(createKey.create(key));
        }
        for (Map.Entry<?, ?> e : map.entrySet()) {
            if (key.equals(mapKeyInspector.getPrimitiveJavaObject(e.getKey()))) {
                return e.getValue();
            }
        }
        return null;
    }

    @Override
    public String getDisplayString(String[] args) {
        return "map_index( " + args[0] + " , " + args[1] + ")";
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] args)
            throws UDFArgumentException {
        if (args.length != 2) {
            throw new UDFArgumentException("Usage : map_index( map, key)");
        }
        if (args[0].getCategory() != Category.MAP
                || args[1].getCategory() != Category.PRIMITIVE) {
            throw new UDFArgumentException("Usage : map_index( map, key) - First argument must be a map, second must be a matching key");
        }
        mapInspector = (MapObjectInspector) args[0];
        mapKeyInspector = (PrimitiveObjectInspector) mapInspector.getMapKeyObjectInspector();
        keyInspector = (PrimitiveObjectInspector) args[1];
        if (mapKeyInspector.getPrimitiveCategory() != keyInspector.getPrimitiveCategory()) {
            throw new UDFArgumentException("Usage : map_index( map, key) - First argument must be a map, second must be a matching key");
        }
        createKey = CreateWithPrimitive.getCreate(mapKeyInspector);
        return mapInspector.getMapValueObjectInspector();
    }

}
