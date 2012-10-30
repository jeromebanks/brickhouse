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
public class MapIndexUDF extends UDF {
	private static final Logger LOG = Logger.getLogger( MapIndexUDF.class);
	
    public Double evaluate( Map<String,Double> map, String key) throws IOException	{
    	/// XXX TODO For now, just assume an array of strings 
    	/// XXX TODO In future, make a GenericUDF, so one can handle multiple types
    	/// XXX
        return map.get(key);
    	
    }

}
