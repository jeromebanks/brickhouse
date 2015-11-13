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
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

import java.util.*;


/**
 * Return a map minus key value pairs from a map that are null.
 *
 * @author otistamp 
 *
 */

@Description(name="map_remove_nulls",
value = "_FUNC_(map, value) - Returns the sorted entries of a map minus key value pairs, the for a given set of keys "
)
public class MapRemoveNullValuesUDF extends GenericUDF {
    private MapObjectInspector mapInspector;
    private StandardMapObjectInspector retValInspector;
    
    private Map stdKeys( Map inspectMap) {
        Map objMap = new HashMap();
        for( Object inspKey: inspectMap.keySet() )   {
            
            Object objKey= ((PrimitiveObjectInspector) mapInspector.getMapKeyObjectInspector()).getPrimitiveJavaObject(inspKey);
            objMap.put( objKey, inspKey);
            
        }
        return objMap;
    }
    

    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        Map hiveMap = mapInspector.getMap(arg0[0].get()  );
        
        /// Convert all the keys to standard keys
        Map stdKeys = stdKeys(hiveMap);


        
        Set stdKeySet = stdKeys.keySet();
        List stdKeyList = new ArrayList(stdKeySet);

        Map retVal = (Map)retValInspector.create();
        TreeMap retValSorted = new TreeMap(retVal);
        for( Object keyObj :stdKeyList ) {
            Object hiveKey = stdKeys.get( keyObj);
            Object hiveVal = hiveMap.get( hiveKey);

            if(hiveKey != null && hiveVal != null) {
                Object keyStd = ObjectInspectorUtils.copyToStandardObject(hiveKey, mapInspector.getMapKeyObjectInspector() );
                Object valStd = ObjectInspectorUtils.copyToStandardObject(hiveVal, mapInspector.getMapValueObjectInspector() );

                retValSorted.put( keyStd, valStd );
            }
        }
        return retValSorted;
    }


    @Override
    public String getDisplayString(String[] arg0) {
        return "map_remove_nulls(" + arg0[0] + ", " + arg0[1] + " )";
    }


    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        ObjectInspector first = arg0[0];
        if(first.getCategory() == Category.MAP) {
            mapInspector =  (MapObjectInspector) first;
        } else {
            throw new UDFArgumentException(" Expecting a map as first argument ");
        }
        

        ObjectInspector mapKeyInspector = mapInspector.getMapKeyObjectInspector();
        if( ! ( mapKeyInspector.getCategory() == Category.PRIMITIVE)) {
            throw new UDFArgumentException(" Expecting a primitive as map key elements.");
        }
        
        retValInspector = (StandardMapObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(first);
        return retValInspector;
    }
}
