package brickhouse.udf.sketch;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import brickhouse.analytics.uniques.SketchSet;

/**
 *  UDF to combine two sketch sets, to estimate size of set union.
 * @author jeromebanks
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
