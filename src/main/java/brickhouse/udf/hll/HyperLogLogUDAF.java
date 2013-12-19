package brickhouse.udf.hll;
/**
 * Copyright 2012,2013 Klout, Inc
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

import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.log4j.Logger;


import brickhouse.analytics.uniques.SketchSet;

/**
 *  Aggregate and return a HyperLogLog.
 *  
 *  Uses Clearspring's Stream-lib project
 *
 */

@Description(name="hyperloglog",
    value = "_FUNC_(x) - Constructs a HyperLogLog to estimate reach for large values  "
)
public class HyperLogLogUDAF extends AbstractGenericUDAFResolver {
  private static final Logger LOG = Logger.getLogger(HyperLogLogUDAF.class);
  static final int PRECISION = 6;


  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    return new HyperLogLogUDAFEvaluator();
  }


  public static class HyperLogLogUDAFEvaluator extends GenericUDAFEvaluator {
    // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
	  private StringObjectInspector inputStrOI;
	  private BinaryObjectInspector partialBufferOI;


    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      super.init(m, parameters);
      /// 
      if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
    	  //// iterate() gets called.. string is passed in
    	  this.inputStrOI = (StringObjectInspector) parameters[0];
      } else { /// Mode m == Mode.PARTIAL2 || m == Mode.FINAL
    	   /// merge() gets called ... map is passed in ..
    	  this.partialBufferOI = (BinaryObjectInspector) parameters[0];
    	  
        		 
      } 
      /// The intermediate result is a map of hashes and strings,
      /// The final result is an array of strings
      if( m == Mode.FINAL || m == Mode.COMPLETE) {
    	  /// for final result
    	  return  PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
      } else { /// m == Mode.PARTIAL1 || m == Mode.PARTIAL2 
    	  return  PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
      }
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      HLLBuffer buff= new HLLBuffer();
      buff.init( PRECISION);
      return buff;
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
        throws HiveException {
      Object strObj = parameters[0];

      if (strObj != null) {
    	  String str = inputStrOI.getPrimitiveJavaObject( strObj);
          HLLBuffer myagg = (HLLBuffer) agg;
          myagg.addItem( str);
      }
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial)
        throws HiveException {
    	/// Partial is going to be a map of strings and hashes 
    	try {
    	
          HLLBuffer myagg = (HLLBuffer) agg;
          byte[] partialBuffer = this.partialBufferOI.getPrimitiveJavaObject(partial);
          myagg.merge( partialBuffer);
    	} catch(Exception e) {
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
    	  throw new HiveException(e);
    	}
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
    	return terminate( agg);
    }
  }


}
