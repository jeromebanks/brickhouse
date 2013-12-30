package brickhouse.udf.hll;
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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.log4j.Logger;

/**
 *   Aggregate multiple HyerLogLog structures together.
 *
 */

@Description(name="union_hyperloglog",
    value = "_FUNC_(x) - Merges multiple hyperloglogs together. "
)
public class UnionHyperLogLogUDAF extends AbstractGenericUDAFResolver {
  private static final Logger LOG = Logger.getLogger(UnionHyperLogLogUDAF.class);


  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    return new MergeHyperLogLogUDAFEvaluator();
  }


  public static class MergeHyperLogLogUDAFEvaluator extends GenericUDAFEvaluator {
    // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
	  private BinaryObjectInspector inputBinaryOI;
	  private BinaryObjectInspector partialBinaryOI;


      


    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      super.init(m, parameters);
      LOG.info(" MergeHyperLogLogUDAF.init() - Mode= " + m.name() );
      for(int i=0; i<parameters.length; ++i) {
        LOG.info(" ObjectInspector[ "+ i + " ] = " + parameters[0]);
      }
      /// 
      if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
    	  //// iterate() gets called.. binary is passed in
    	  this.inputBinaryOI = (BinaryObjectInspector) parameters[0];
    	  
    	  
      } else { /// Mode m == Mode.PARTIAL2 || m == Mode.FINAL
    	   /// merge() gets called ... binary is passed in ..
    	  this.partialBinaryOI = (BinaryObjectInspector) parameters[0];
        		 
      } 
      /// The intermediate result is a map of hashes and strings,
      /// The final result is an array of strings
      if( m == Mode.FINAL || m == Mode.COMPLETE) {
    	  /// for final result
    	  return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
      } else { /// m == Mode.PARTIAL1 || m == Mode.PARTIAL2 
    	  return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
      }
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      HLLBuffer buff= new HLLBuffer();
      buff.init( HyperLogLogUDAF.DEFAULT_PRECISION);
      return buff;
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
    		throws HiveException {
    	try {
    		Object blobObj = parameters[0];

    		if (blobObj != null) {
    			///ByteArrayRef bref = this.inputBinaryOI.getPrimitiveJavaObject(blobObj);
    			byte[] bref = this.inputBinaryOI.getPrimitiveJavaObject(blobObj);
    			HLLBuffer hllBuff = (HLLBuffer) agg;
    			///hllBuff.merge( bref.getData());
    			if(bref != null)
    				hllBuff.merge( bref);

    		}
    	} catch(Exception e) {
    		LOG.error("Error",e);
    		throw new HiveException(e);
    	}
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial)
        throws HiveException {
    	try {
    	/// Partial is going to be binary
        HLLBuffer myagg = (HLLBuffer) agg;
        
        byte[] bref = this.partialBinaryOI.getPrimitiveJavaObject(partial);
        myagg.merge( bref);
    	} catch(Exception e) {
    		LOG.error("Error",e);
    		throw new HiveException(e);
    	}
        
    }

    @Override
    public void reset(AggregationBuffer buff) throws HiveException {
      HLLBuffer hllBuff = (HLLBuffer) buff;
      hllBuff.reset();
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
    	try {
    		HLLBuffer myagg = (HLLBuffer) agg;
    		return myagg.getPartial();
    	} catch(Exception e) {
    		LOG.error("Error",e);
    		throw new HiveException(e);
    	}
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
    	return terminate(agg);
    }
  }


}
