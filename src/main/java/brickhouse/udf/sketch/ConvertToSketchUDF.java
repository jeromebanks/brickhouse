package brickhouse.udf.sketch;

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
