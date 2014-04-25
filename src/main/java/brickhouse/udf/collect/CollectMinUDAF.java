package brickhouse.udf.collect;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import brickhouse.udf.collect.CollectMaxUDAF.MapCollectMaxUDAFEvaluator;


@Description(name="collect_min",
    value = "_FUNC_(x, val, n) - Returns an map of the N min numeric values in the aggregation group "
)
public class CollectMinUDAF extends AbstractGenericUDAFResolver {

	  @Override
	  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
	      throws SemanticException {
	    return new MapCollectMinUDAFEvaluator();
	  }
	  
	  public static class MapCollectMinUDAFEvaluator extends MapCollectMaxUDAFEvaluator {
		  
		  public MapCollectMinUDAFEvaluator() {
			  super(false);
		  }
	  }

}
