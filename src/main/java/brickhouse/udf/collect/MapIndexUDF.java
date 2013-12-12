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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.log4j.Logger;


/**
 *   Workaround for the Hive bug 
 *   https://issues.apache.org/jira/browse/HIVE-1955
 *   
 *  FAILED: Error in semantic analysis: Line 4:3 Non-constant expressions for array indexes not supported key
 *  
 *   
 *  Use instead of [ ] syntax,   
 *
 *
 */
public class MapIndexUDF extends GenericUDF {
	private static final Logger LOG = Logger.getLogger( MapIndexUDF.class);
	private PrimitiveObjectInspector keyInspector;
	private MapObjectInspector mapInspector;
	
    public Double evaluate( Map<String,Double> map, String key) throws IOException	{
    	/// XXX TODO For now, just assume an array of strings 
    	/// XXX TODO In future, make a GenericUDF, so one can handle multiple types
    	/// XXX
        return map.get(key);
    	
    }

    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        Object mapObj = arg0[0].get();
        Object keyObj = arg0[1].get();
       
        Object mapVal = mapInspector.getMapValueElement(mapObj, keyObj);
        
        return mapVal;
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "map_index( " + arg0[0] + " , " + arg0[1] + ")";
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        if( arg0.length != 2) {
            throw new UDFArgumentException("Usage : map_index( map, key)");
        }
        if( arg0[0].getCategory() != Category.MAP 
                || arg0[1].getCategory() != Category.PRIMITIVE ) {
            throw new UDFArgumentException("Usage : map_index( map, key) - First argument must be a map, second must be a matching key");
        }
        mapInspector = (MapObjectInspector) arg0[0];
        keyInspector = (PrimitiveObjectInspector) arg0[1];
        if( ((PrimitiveObjectInspector)mapInspector.getMapKeyObjectInspector()).getPrimitiveCategory() 
                != keyInspector.getPrimitiveCategory() ) {
            throw new UDFArgumentException("Usage : map_index( map, key) - First argument must be a map, second must be a matching key");
        }
        return mapInspector.getMapValueObjectInspector();
    }

}
