package brickhouse.hbase;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.log4j.Logger;

/**
 *   Insert into HBase by doing bulk puts from an aggregate function call.
 *
 */

@Description(name="hbase_batch_put",
value = "_FUNC_(t,k,v,<batchsize>) - Perform batch HBase updates of a table " 
)
public class BatchPutUDAF extends AbstractGenericUDAFResolver {
	private static final Logger LOG = Logger.getLogger( BatchPutUDAF.class);
	private static Configuration config = new Configuration(true);
	
	static private byte[] FAMILY = "c".getBytes();
	static private byte[] QUALIFIER = "q".getBytes();
	



	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
			throws SemanticException {
		for(int i=0; i<parameters.length; ++i) {
			LOG.info(" BATCH PUT PARAMETERS : " + i  + " -- " + parameters[i].getTypeName() + " cat = " + parameters[i].getCategory());
		}
		String strTypeName = PrimitiveObjectInspectorUtils.getTypeNameFromPrimitiveJava(String.class);
		String intTypeName = PrimitiveObjectInspectorUtils.getTypeNameFromPrimitiveJava(Integer.class);
		
		if (parameters.length != 3 && parameters.length != 4) {
					LOG.warn(" param length not right; Expecting hbase_batch_put( string tablename, string key, string val, <optional> batch size)");
			///throw new UDFArgumentTypeException(parameters.length - 1,
					///"Expecting hbase_batch_put( string tablename, string key, string val, <optional> batch size)");
		}
		
		if( !parameters[0 ].getTypeName().equals(strTypeName) 
				|| !parameters[1 ].getTypeName().equals(strTypeName) 
				|| !parameters[2 ].getTypeName().equals(strTypeName) ) {
					LOG.warn("params not string,Expecting hbase_batch_put( string tablename, string key, string val, <optional> batch size)");
			///throw new UDFArgumentTypeException(parameters.length - 1,
					///"Expecting hbase_batch_put( string tablename , string key, string val, <optional> batch size)");
		}
		if(parameters.length == 4) {
			if( ! parameters[3].getTypeName().equals(intTypeName) ) {
			   ///throw new UDFArgumentTypeException(parameters.length - 1,
					///"Expecting batch_put( string tablename, string key, string val, <optional> batch size)");
					LOG.warn(" batch size not int;Expecting batch_put( string tablename, string key, string val, <optional> batch size)");
				
			}
		}
		
		return new BatchPutUDAFEvaluator();
	}
	
	static public class PutBuffer implements AggregationBuffer{
		public String tableName;
		public List<Put> putList;
		
		public PutBuffer(String tablename) { tableName = tablename; }
		
		public void reset() { putList = new ArrayList<Put>(); }
		
		public void addKeyValue( String key, String val) throws HiveException{
			Put thePut = new Put(key.getBytes());
			thePut.add( FAMILY, QUALIFIER, val.getBytes());
			putList.add( thePut);
		}
	}
	

	public static class BatchPutUDAFEvaluator extends GenericUDAFEvaluator {
		private int batchSize = 10000;
		private int numPutRecords = 0;/// XXX TODO Count 
		private String zookeeperQuorum;
		
		// For PARTIAL1 and COMPLETE: ObjectInspectors for original data
		private StringObjectInspector inputKeyOI;
		private StringObjectInspector inputValOI;
		// For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
		// of objs)
		private StandardListObjectInspector listKVOI;
		private String tablename;
		
		private HTable table;


		private HTable initHTable(String tablename) throws IOException {
			if(table == null) {
				/// XXX why isn't zookeeper quorum set ???
				/// XXX How to get 
				if( zookeeperQuorum != null )
			       config.set("hbase.zookeeper.quorum", zookeeperQuorum);
		       table =   new HTable( HBaseConfiguration.create(config), tablename);
				table.setAutoFlush(false);
			}
			return table;
		}

		public ObjectInspector init(Mode m, ObjectInspector[] parameters)
				throws HiveException {
			super.init(m, parameters);
			// init output object inspectors
			///  input will be key, value and batch size
			if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
				if( ! ( parameters[0] instanceof ConstantObjectInspector)) {
				   throw new HiveException("Tablename must be a constant");
				}
				tablename = ((ConstantObjectInspector) parameters[0]).getWritableConstantValue().toString();
				try {
					initHTable(tablename);
				} catch (IOException e) {
					throw new HiveException(e);
				}
				
				
				inputKeyOI = (StringObjectInspector) parameters[1];
				inputValOI = (StringObjectInspector) parameters[2];
				
				
				if( parameters.length == 4) {
					if(!( parameters[3] instanceof ConstantObjectInspector) ) {
						throw new HiveException("Batch size must be a constant");
					}
					ConstantObjectInspector constInspector = (ConstantObjectInspector) parameters[3];
					Object batchObj = constInspector.getWritableConstantValue();
					batchSize = Integer.valueOf(batchObj.toString());
				}
				if( parameters.length == 5) {
					if(!( parameters[4] instanceof ConstantObjectInspector) ) {
						throw new HiveException("Zookeeper quorum must be a constant");
					}
					ConstantObjectInspector constInspector = (ConstantObjectInspector) parameters[4];
					Object constObj = constInspector.getWritableConstantValue();
					zookeeperQuorum = constObj.toString();
				}
				
			} else {
				///  input will be our List of lists
				listKVOI = (StandardListObjectInspector) parameters[0];
			}
			
			
			if( m == Mode.PARTIAL1 || m  == Mode.PARTIAL2) {
			   return ObjectInspectorFactory
						.getStandardListObjectInspector(
								ObjectInspectorFactory.getStandardListObjectInspector(
										PrimitiveObjectInspectorFactory.javaStringObjectInspector ) );
			} else {
				/// Otherwise return a message
				return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			}
		}

		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			PutBuffer buff= new PutBuffer( tablename);
			reset(buff);
			return buff;
		}

		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters)
				throws HiveException {
			String key = inputKeyOI.getPrimitiveJavaObject(parameters[1]);
			String val = inputValOI.getPrimitiveJavaObject(parameters[2]);
			
			PutBuffer kvBuff = (PutBuffer) agg;
			kvBuff.addKeyValue( key,val);

			if(kvBuff.putList.size() >= batchSize) {
				batchUpdate( kvBuff);
			}
		}
		
		protected void batchUpdate( PutBuffer  kvBuff) throws HiveException { 
			try {
				HTable htable = initHTable( kvBuff.tableName);
				
				htable.put( kvBuff.putList);
				htable.flushCommits();
				
			} catch (IOException e) {
				throw new HiveException(e);
			}
		}

		@Override
		public void merge(AggregationBuffer agg, Object partial)
				throws HiveException {
			PutBuffer myagg = (PutBuffer) agg;
			List<Object> partialResult = (List<Object>)this.listKVOI.getList(partial);
			ListObjectInspector subListOI = (ListObjectInspector) listKVOI.getListElementObjectInspector();
		
			List first = subListOI.getList( partialResult.get(0));
			String tableName = ((StringObjectInspector)(subListOI.getListElementObjectInspector())).getPrimitiveJavaObject(first.get(0));
			myagg.tableName = tableName;
			
			for(int i=1; i< partialResult.size(); ++i) {
				
			   List kvList = subListOI.getList( partialResult.get(i));
			   String key = ((StringObjectInspector)(subListOI.getListElementObjectInspector())).getPrimitiveJavaObject(kvList.get(0));
			   String val = ((StringObjectInspector)(subListOI.getListElementObjectInspector())).getPrimitiveJavaObject(kvList.get(1));
			   
			   myagg.addKeyValue( key, val);
			   
			}
			
			if(myagg.putList.size() >= batchSize) {
				batchUpdate( myagg);
			}
		}

		@Override
		public void reset(AggregationBuffer buff) throws HiveException {
			PutBuffer putBuffer = (PutBuffer) buff;
			putBuffer.reset();
		}

		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			PutBuffer myagg = (PutBuffer) agg;
			batchUpdate( myagg);
			return "Finished Batch updates " ; /// XXX TODO -count how many updated

		}


		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			PutBuffer myagg = (PutBuffer) agg;
			
			
			ArrayList<List<String>> ret = new ArrayList<List<String>>();
			ArrayList tname = new ArrayList<String>();
			tname.add( tablename);
			ret.add( tname);
			
			for(Put thePut : myagg.putList) {
				/// XXX TODO XXX TODO Abstract to include all columns ...
				ArrayList<String> kvList = new ArrayList<String>();
				kvList.add( new String(thePut.getRow() )  );
			    Map<byte[],List<KeyValue>> familyMap = thePut.getFamilyMap();
			    for( List<KeyValue> innerList : familyMap.values() ) {
			    	for(KeyValue kv : innerList) {
			    		kvList.add( new String( kv.getValue() ));
			    	}
			    }
			    ret.add( kvList);
			}
			
			return ret;
		}
	}



}
