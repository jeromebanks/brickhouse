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


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.yarn.webapp.example.MyApp;
import org.apache.log4j.Logger;

/**
 *  Same as the normal collect, but cap the number of elements collected to a certain number, to avoid 
 *    out of memory errors.
 *    
 *    Implemented currently only for arrays.
 *
 */

@Description(name="collect_capped",
value = "_FUNC_(x,limit) - Returns an array of all the elements in the aggregation group, capped at a certain limit." 
)
public class CollectCappedUDAF extends AbstractGenericUDAFResolver {
   public static final Logger LOG = Logger.getLogger(CollectCappedUDAF.class);



	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
			throws SemanticException {
		if (parameters.length != 2) {
			throw new UDFArgumentTypeException(parameters.length - 1,
					"collect_capped expects two parameters; the field being collected and the max number of items");
		}
		return new CappedCollectUDAFEvaluator();
	}

	public static class CappedCollectUDAFEvaluator extends GenericUDAFEvaluator {
		// For PARTIAL1 and COMPLETE: ObjectInspectors for original data
		private ObjectInspector inputOI;
		// For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
		// of objs plus the cap)
		private StructObjectInspector internalMergeOI;
		private IntObjectInspector cappedLimitOI;
		private StructField cappedLimitField;
		private ListObjectInspector cappedListOI;
		private StandardListObjectInspector retListOI;
		private StructField cappedListField;
		private int capLimit = 1000000;


		static class CappedBuffer implements AggregationBuffer {
			int capLimit = 1000000;
			ArrayList collectArray = new ArrayList();
		}

		public ObjectInspector init(Mode m, ObjectInspector[] parameters)
				throws HiveException {
			super.init(m, parameters);
			// init output object inspectors
			// The output of a partial aggregation is a list
		   	
			/// the end output is a list
			if (m == Mode.PARTIAL1 || m == Mode.COMPLETE ) {
			   if( parameters.length != 2
					|| !(parameters[1] instanceof ConstantObjectInspector)
					|| !(parameters[1] instanceof IntObjectInspector)) {
				  for(int i=0; i< parameters.length; ++i) {
			        LOG.error( "   Parameters[" + i + "] is " + parameters[i]);
				  } 
			      throw new HiveException(" Collect Capped takes two parameters; the column being collected and a constant specifying the maximum number of elements to capture.");
			    }

			    ConstantObjectInspector constInspector = (ConstantObjectInspector) parameters[1];
			    capLimit = ((IntWritable) constInspector.getWritableConstantValue()).get();
			    LOG.info(" Capping number of elements in list at " + capLimit + " values.");
			
			
				inputOI = parameters[0];
			    retListOI =  ObjectInspectorFactory
						.getStandardListObjectInspector( 
								ObjectInspectorUtils.getStandardObjectInspector(inputOI ));
				if( m== Mode.COMPLETE ) {
				   internalMergeOI = null;
				  return retListOI;
				} else {
					///// Mode == PARTIAL1 
					//// Need to return a Struct with the limit and the list
					List<String> structFieldNames = new ArrayList<String>();
					List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
					
					structFieldNames.add("cap");
					structFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaIntObjectInspector );

					structFieldNames.add("list");
					structFieldObjectInspectors.add( retListOI );
					internalMergeOI = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
				
				    return internalMergeOI;
				}
			} else {
				///if ( m == Mode.PARTIAL2 || m == Mode.FINAL) {
					inputOI = null;  /// Don't use this for this case
					internalMergeOI = (StructObjectInspector)parameters[0];
					cappedLimitField = internalMergeOI.getStructFieldRef("cap");
					cappedLimitOI = (IntObjectInspector) cappedLimitField.getFieldObjectInspector();
					cappedListField = internalMergeOI.getStructFieldRef("list");
					cappedListOI = (ListObjectInspector) cappedListField.getFieldObjectInspector();

					retListOI = ObjectInspectorFactory
							.getStandardListObjectInspector( 
									ObjectInspectorUtils.getStandardObjectInspector(cappedListOI.getListElementObjectInspector()));
					return retListOI;
				///}
			}
		}

		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			AggregationBuffer buff= new CappedBuffer();
			reset(buff);
			return buff;
		}

		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters)
				throws HiveException {
			Object p = parameters[0];

			if (p != null) {
				CappedBuffer myagg = (CappedBuffer) agg;
				putIntoSet(p, myagg, this.inputOI);
			}
		}

		@Override
		public void merge(AggregationBuffer agg, Object partial)
				throws HiveException {
			CappedBuffer myagg = (CappedBuffer) agg;
			
			int capLimit = cappedLimitOI.get( internalMergeOI.getStructFieldData(partial, cappedLimitField) );
			myagg.capLimit = capLimit;
			Object listObj = internalMergeOI.getStructFieldData(partial, cappedListField);
			int listSize = cappedListOI.getListLength(listObj);
			for(int i=0; i<listSize; ++i) {
				if( myagg.collectArray.size() < myagg.capLimit) {
				   Object uninsp = cappedListOI.getListElement(listObj, i);
				   putIntoSet(uninsp, myagg, cappedListOI.getListElementObjectInspector());
				} else {
					break;
				}
			}
		}

		@Override
		public void reset(AggregationBuffer buff) throws HiveException {
			CappedBuffer arrayBuff = (CappedBuffer) buff;
			arrayBuff.capLimit = capLimit;
			arrayBuff.collectArray = new ArrayList();
		}

		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			CappedBuffer myagg = (CappedBuffer) agg;
			int listSize = myagg.collectArray.size();
			Object newList = retListOI.create( listSize);
			for(int i=0; i<listSize; ++i) {
               Object colVal = myagg.collectArray.get(i);
			   retListOI.set( newList,i,colVal);
			}
			return newList;
		}

		private void putIntoSet(Object p, CappedBuffer myagg, ObjectInspector oi) {
			if( myagg.collectArray.size() < myagg.capLimit ) {
			   Object standardP = ObjectInspectorUtils.copyToStandardObject(p, oi );
			   myagg.collectArray.add( standardP);
			}
		}

		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			CappedBuffer myagg = (CappedBuffer) agg;
			Object[] structArr = new Object[2];
			structArr[0]  = new Integer( myagg.capLimit);
		    structArr[1]  = myagg.collectArray;
			
			return structArr;
		}
	}


}
