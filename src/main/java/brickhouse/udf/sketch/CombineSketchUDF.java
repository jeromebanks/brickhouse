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
 *  UDF to combine two sketch sets, to estimate size of set union.
 *
 */
@Description(name="combine_sketch",
    value = "_FUNC_(x) - Combine two sketch sets. "
)
public class CombineSketchUDF extends UDF {
	
	
	public List<String> evaluate( List<String> strList1, List<String> strList2) {
		SketchSet sketch1 = new SketchSet();
		
		if(strList1 != null) {
			for(String item : strList1) {
				sketch1.addItem( item);
			}
		}
		
		SketchSet sketch2 = new SketchSet();
		
		if(strList2 != null) {
			for(String item : strList2) {
				sketch2.addItem( item);
			}
		}
		sketch1.combine( sketch2);
		
		return sketch1.getMinHashItems();
	}

}
