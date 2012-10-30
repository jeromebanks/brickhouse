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

/*
 *  Take an array of strings, and convert to a truncated array,
 *   representing a sketch set of those strings.
 *   
 *   Useful for converting legacy lists of high-reach users,
 *    and converting to sketch sets.
 */
@Description(name="convert_to_sketch",
value = "_FUNC_(x) - Truncate a large array of strings, and return a list of strings representing a sketch of those items "
)
public class ConvertToSketchUDF extends UDF {

	public List<String> evaluate(List<Object> objList) {
		SketchSet sketch = new SketchSet();
	
		for(Object item : objList) {
			sketch.addItem(item.toString());
		}
		return sketch.getMinHashItems();
	}
}
