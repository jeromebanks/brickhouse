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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.TreeMap;

/**
 * Cast an Map to the string to string map
 * <p/>
 * Based on CastArrayUDF.
 */
public class CastMapUDF extends GenericUDF {
    private static final Logger LOG = Logger.getLogger(CastMapUDF.class);
    private MapObjectInspector mapInspector;


    public Map<String, String> evaluate(Map<Object, Object> strMap) {
        Map<String, String> newMap = new TreeMap<String, String>();
        for (Object keyObj : strMap.keySet()) {
            newMap.put(keyObj.toString(), strMap.get(keyObj).toString());
        }
        return newMap;
    }

    @Override
    public Map<String, String> evaluate(DeferredObject[] arg0) throws HiveException {
        Map argMap = mapInspector.getMap(arg0[0].get());
        if (argMap != null)
            return evaluate(argMap);
        else
            return null;
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "cast_map()";
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        this.mapInspector = (MapObjectInspector) arg0[0];
        LOG.info(" Cast Map input type is " + mapInspector +
                " key = " + mapInspector.getMapKeyObjectInspector().getTypeName() +
                " value = " + mapInspector.getMapValueObjectInspector().getTypeName());
        ObjectInspector returnType = ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return returnType;
    }
}
