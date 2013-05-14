package brickhouse.udf.sketch;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import brickhouse.analytics.uniques.SketchSet;

/**
 * Compute the Jaccard similarity of two sketch sets.
 * 
 * Jaccard Similarity is defined as the size of the intersection of two sets divided by the 
 *   size of the union of the sets. Since sketches are only approximate measures, this
 *   calculation only makes sense when the sets are roughly the same size.
 *
 */
@Description(name="set_similarity",
value = "_FUNC_(a,b) - Compute the Jaccard set similarity of two sketch sets. "
)
public class SetSimilarityUDF extends UDF {

	public Double evaluate( List<String> a, List<String> b) {
		if( a == null || b == null ) 
			return null;
		if( a.size() ==0 || b.size() == 0 ) {
			return 0.0;
		}
		SketchSet sketchA = new SketchSet();
		SketchSet sketchB = new SketchSet();
		SketchSet sketchAUB = new SketchSet();
		
		
		for(String aStr : a) {
			sketchA.addItem( aStr);
			sketchAUB.addItem( aStr);
		}
		for(String bStr : b) {
			sketchB.addItem( bStr);
			sketchAUB.addItem( bStr);
		}
		
		
		
		double aEst = sketchA.estimateReach();
		double bEst = sketchB.estimateReach();
		double aubEst = sketchAUB.estimateReach();
		
		/// Intersection is 
		double ainterb =  aEst + bEst - aubEst;
		double sim = ainterb/aubEst;
		
		return sim;
	}
}
