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

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde.serdeConstants;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;


/**
 *  Perform a batch put into HBase but as a UDF,
 *  given a key and and array of values that get inserted into single column
 *
 * @author Marcin Michalski
 */
@Description(name="hbase_array_value_put",
value = "_FUNC_(config, key, val_array) - Do a batch HBase Put on a table, given single keys and array of column values"
)
public class ArrayValuePutUDF extends GenericUDF {
	private static final Logger LOG = Logger.getLogger( ArrayValuePutUDF.class);
	private StringObjectInspector keyInspector;
	private ListObjectInspector listValInspector;
	private StringObjectInspector valueInspector;
	private Map<String,String> configMap;
	
	private HTable table;
    private Reporter reporter;

	
	///public List<String> evaluate( Map<String,String> config, List<String> keys) {
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		try {
		   if(table == null)
	         table = HTableFactory.getHTable( configMap);

		   String key = keyInspector.getPrimitiveJavaObject(arg0[1].get() );
           Object listValObj = arg0[2].get();
	       int listValLen = listValInspector.getListLength( listValObj);

           List<String> columnValues = new ArrayList<String>();
           for(int i=0; i<listValLen; ++i) {
               Object uninspValue = listValInspector.getListElement(listValObj, i);
               columnValues.add(valueInspector.getPrimitiveJavaObject(uninspValue));
	       }

           if(key != null && columnValues.size() > 0) {
               Put thePut = new Put(key.getBytes());

               //Serialize values into byte array
               thePut.add(configMap.get(HTableFactory.FAMILY_TAG).getBytes(),
                       configMap.get(HTableFactory.QUALIFIER_TAG).getBytes(),
                       WritableUtils.toByteArray(ValueSerde.serialize(columnValues)) );


               table.put(thePut);
               table.flushCommits();

               getReporter().incrCounter(ArrayValuePutUDFCounter.NUMBER_OF_SUCCESSFUL_PUTS, 1);
           } else {
               System.out.println("Null key or no values for key: " + key + " number of values: " + columnValues.size());
               getReporter().incrCounter(ArrayValuePutUDFCounter.NULL_KEY_OR_VALUE_INSERT_FAILURE, 1);
           }
	      
	       return "Put " + listValLen + " values for key: " + key + " into " + table.getName() + " Family "
                   + configMap.get( HTableFactory.FAMILY_TAG ) + " ; Qualifier = "
                   + configMap.get( HTableFactory.QUALIFIER_TAG );
		} catch(Exception exc ) {
			 LOG.error(" Error while trying HBase PUT ",exc);
			 throw new RuntimeException(exc);
		}
	}

	@Override
	public String getDisplayString(String[] arg0) {
		return "hbase_batch_put_array( " + arg0[0] + " ) ";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0)
			throws UDFArgumentException {
       if( arg0.length !=3 ) {
    	   throw new UDFArgumentException(" hbase_array_value_put expects a config map, a key, and an array of values [0]");
       }
       if(arg0[0].getCategory() != Category.MAP)  {
    	   throw new UDFArgumentException(" hbase_array_value_put expects a config map, a key, and an array of values [1]");
       }
       configMap = HTableFactory.getConfigFromConstMapInspector(arg0[0]);
	   HTableFactory.checkConfig( configMap);
       
       
       if(! arg0[1].getTypeName().equals(serdeConstants.STRING_TYPE_NAME) ) {
    	   throw new UDFArgumentException(" hbase_array_value_put expects a config map, key of type string, and an array of values [2]");
       }
       keyInspector = (StringObjectInspector) arg0[1];

       if(arg0[2].getCategory() != Category.LIST) {
    	   throw new UDFArgumentException(" hbase_array_value_put expects a config map, a key, and an array of values [3]");
       }

       listValInspector = (ListObjectInspector) arg0[2];
       if(listValInspector.getListElementObjectInspector().getCategory() != Category.PRIMITIVE
         || ((PrimitiveObjectInspector)(listValInspector.getListElementObjectInspector())).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING ) {
           throw new UDFArgumentException(" hbase_array_value_put expects a config map, a key, and an array of string values [4]");
        }

       valueInspector = (StringObjectInspector) listValInspector.getListElementObjectInspector();

       return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	}

    private Reporter getReporter() throws HiveException {
        try {
            if (reporter == null) {
                Class clazz = Class.forName("org.apache.hadoop.hive.ql.exec.MapredContext");
                Method staticGetMethod = clazz.getMethod("get");
                Object mapredObj = staticGetMethod.invoke(null);
                Class mapredClazz = mapredObj.getClass();
                Method getReporter = mapredClazz.getMethod("getReporter");
                Object reporterObj = getReporter.invoke(mapredObj);

                reporter = (Reporter) reporterObj;
            }

            return reporter;
        } catch(Exception e) {
            throw new HiveException("Error while accessing Hadoop Counters", e);
        }
    }

    public static class ValueSerde {
        public static Writable serialize(List<String> values) throws IOException {
            Writable[] content = new Writable[values.size()];
            for (int i = 0; i < content.length; i++) {
                content[i] = new Text(values.get(i));
            }

            return new ArrayWritable(Text.class, content);
        }

        public static List<String> deserialize(ArrayWritable writable) {
            Writable[] writables = ((ArrayWritable) writable).get();
            List<String> list = new ArrayList<String>(writables.length);
            for (Writable wrt : writables) {
                list.add(((Text) wrt).toString());
            }
            return list;
        }
    }

    private static enum ArrayValuePutUDFCounter {
        NULL_KEY_OR_VALUE_INSERT_FAILURE, NUMBER_OF_SUCCESSFUL_PUTS;
    }
}
