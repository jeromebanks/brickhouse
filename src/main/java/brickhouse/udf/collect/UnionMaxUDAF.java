package brickhouse.udf.collect;
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

import java.lang.reflect.Field;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.log4j.Logger;

import brickhouse.udf.collect.CollectMaxUDAF.MapCollectMaxUDAFEvaluator;


/**
 *  UDAF to merge a union of maps,
 *    but only hold on the keys with the top 20 values
 */

@Description(name="union_max",
    value = "_FUNC_(x,  n) - Returns an map of the union of maps of max N elements in the aggregation group "
)
public class UnionMaxUDAF extends AbstractGenericUDAFResolver {
  private static final Logger LOG = Logger.getLogger(UnionMaxUDAF.class);


  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    return new MapUnionMaxUDAFEvaluator();
  }


  static public class MapUnionMaxUDAFEvaluator extends MapCollectMaxUDAFEvaluator {
	  protected MapObjectInspector inputMapOI;

	 public MapUnionMaxUDAFEvaluator() {
	    super(true);
	 }
	 
	 public MapUnionMaxUDAFEvaluator(boolean desc) {
		 super(desc);
	 }
	 
	 
	/**
	 * Need to define init slightly differently, because argument is going to be 
	 *     a single map, not key and value arguments
	 */
	@Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      ////GenericUDAFEvaluator.init(m, parameters);
      //// XXX Ay Yah !!! Cannot call init directly, because we have different arguments
      //// XXX But we need to set the mode on the evaluator ..
	  setMode( m);
      LOG.info(" UnionMaxUDAF.init() - Mode= " + m.name() );
      for(int i=0; i<parameters.length; ++i) {
        LOG.info(" ObjectInspector[ "+ i + " ] = " + parameters[0]);
      }

      // init output object inspectors
      // The output of a partial aggregation is a map
      if (m == Mode.PARTIAL1 ||  m == Mode.COMPLETE) {
    	  inputMapOI = (MapObjectInspector)parameters[ 0];
    	  inputKeyOI = (PrimitiveObjectInspector) inputMapOI.getMapKeyObjectInspector();
    	  inputValOI = (PrimitiveObjectInspector) inputMapOI.getMapValueObjectInspector();

    	  
    	  internalMergeOI = ObjectInspectorFactory.getStandardMapObjectInspector(inputKeyOI, inputValOI);
    			  
    	  return internalMergeOI;
    			  
      } else {
          internalMergeOI = (StandardMapObjectInspector) parameters[0];
          inputKeyOI = (PrimitiveObjectInspector) internalMergeOI.getMapKeyObjectInspector();
          inputValOI = (PrimitiveObjectInspector) internalMergeOI.getMapValueObjectInspector();
          
          return internalMergeOI;
        }
    }
	
	protected void setMode( Mode m) {
	    try {
			Field modeField = GenericUDAFEvaluator.class.getDeclaredField("mode");
			if(!modeField.isAccessible()) {
			   modeField.setAccessible(true);	
			}
			modeField.set( this, m);
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
	}

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      MapAggBuffer buff= new MapAggBuffer();
      reset(buff);
      return buff;
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
        throws HiveException {
    	Map inMap = inputMapOI.getMap( parameters[0]);
    	for( Object k:  inMap.keySet()  ) {
    		Object v =  inMap.get( k);
    		if (k == null || v == null) {
    			throw new HiveException("Key or value is null.  k = " + k + " , v = " + v);
    		}

    		if (k != null) {
    			MapAggBuffer myagg = (MapAggBuffer) agg;
    			putIntoSet(k, v, myagg);
    		}
    	}
    }

  }

}
