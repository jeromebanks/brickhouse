package brickhouse.udf.sketch;
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


import java.util.List;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import brickhouse.analytics.uniques.SketchSet;

/**
 *  Interpret a list of strings as a sketch_set
 *  and return an estimated reach number
 *
 */
@Description(name="estimated_reach",
    value = "_FUNC_(x) - Estimate reach from a  sketch set of Strings. "
)
public class EstimatedReachUDF extends UDF {
	
	
	public Long evaluate( List<String> strList, int maxItems) {
		if(strList != null ) {
			if(strList.size() < maxItems) {
				return (long)strList.size();
			} else {
				return (long)SketchSet.EstimatedReach( strList.get( strList.size() -1), maxItems);
			}
		} else {
			return 0l;
		}
	}
	
	public Long evaluate( List<String> strList) {
		return evaluate( strList, SketchSet.DEFAULT_MAX_ITEMS );
	}
	

}
