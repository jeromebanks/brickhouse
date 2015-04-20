package brickhouse.udf.sketch;

import brickhouse.analytics.uniques.SketchSet;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.List;

/**
 * Compute the Jaccard similarity of two sketch sets.
 * <p/>
 * Jaccard Similarity is defined as the size of the intersection of two sets divided by the
 * size of the union of the sets. Since sketches are only approximate measures, this
 * calculation only makes sense when the sets are roughly the same size.
 */
@Description(name = "set_similarity",
        value = "_FUNC_(a,b) - Compute the Jaccard set similarity of two sketch sets. "
)
public class SetSimilarityUDF extends UDF {

    public Double evaluate(List<String> a, List<String> b) {
        if (a == null || b == null)
            return null;
        if (a.size() == 0 || b.size() == 0) {
            return 0.0;
        }
        /// For now, assume min sketch size is 5000...
        /// otherwise it is better to use array_intersect
        /// XXX TODO convert to GenericUDF, so that it can be passed in
        ///  as an argument
        int sketchSize = Math.max(a.size(), b.size());
        if (sketchSize < SketchSetUDAF.DEFAULT_SKETCH_SET_SIZE)
            sketchSize = SketchSetUDAF.DEFAULT_SKETCH_SET_SIZE;

        SketchSet sketchA = new SketchSet(sketchSize);
        SketchSet sketchB = new SketchSet(sketchSize);
        SketchSet sketchAUB = new SketchSet(sketchSize);


        for (String aStr : a) {
            sketchA.addItem(aStr);
            sketchAUB.addItem(aStr);
        }
        for (String bStr : b) {
            sketchB.addItem(bStr);
            sketchAUB.addItem(bStr);
        }

        double aEst = sketchA.estimateReach();
        double bEst = sketchB.estimateReach();
        double aubEst = sketchAUB.estimateReach();

        /// Intersection is
        double ainterb = aEst + bEst - aubEst;
        double sim = ainterb / aubEst;

        return sim;
    }
}
