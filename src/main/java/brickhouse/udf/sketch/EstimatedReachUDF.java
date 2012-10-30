package brickhouse.udf.sketch;


/**
 *    Licensed to the Apache Software Foundation (ASF) under one
 *    or more contributor license agreements.  See the NOTICE file
 *    distributed with this work for additional information
 *    regarding copyright ownership.  The ASF licenses this file
 *    to you under the Apache License, Version 2.0 (the
 *    "License"); you may not use this file except in compliance
 *    with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing,
 *    software distributed under the License is distributed on an
 *    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *    KIND, either express or implied.  See the License for the
 *    specific language governing permissions and limitations
 *    under the License.
 *
 **/


import java.util.List;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import brickhouse.analytics.uniques.SketchSet;

/**
 *  Interpret a list of strings as a sketch_set
 *  and return an estimated reach number
 * @author jeromebanks
 *
 */
@Description(name="estimated_reach",
    value = "_FUNC_(x) - Estimate reach from a  sketch set of Strings. "
)
public class EstimatedReachUDF extends UDF {
	
	
	public Long evaluate( List<String> strList) {
		if(strList != null ) {
			SketchSet sketch = new SketchSet();
		
			for(String item : strList) {
				sketch.addItem( item);
			}
			return (long)sketch.estimateReach();
		} else {
			return 0l;
		}
	}

}
