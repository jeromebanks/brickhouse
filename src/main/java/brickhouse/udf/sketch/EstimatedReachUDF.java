package brickhouse.udf.sketch;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import brickhouse.analytics.uniques.SketchSet;

/**
 *  Interpret a list of strings as a sketch_set
 *  and return an estimated number
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
