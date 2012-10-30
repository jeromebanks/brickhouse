package brickhouse.udf.json;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

/**
 *  Generate a JSON string from a Map
 *
 */
@Description(name="to_json",
    value = "_FUNC_(a,b) - Returns a JSON string from a Map of values"
)
public class ToJsonUDF extends GenericUDF {
	private InspectorHandle inspHandle;
	private interface  InspectorHandle {
		abstract public String generateJson( Object obj);
	};


	private class MapInspectorHandle implements InspectorHandle {
		private MapObjectInspector mapInspector;
		private InspectorHandle keyInspector;
		private InspectorHandle valueInspector;


		public MapInspectorHandle( MapObjectInspector mInsp) throws UDFArgumentException {
			mapInspector = mInsp;
			keyInspector = GenerateInspectorHandle( mInsp.getMapKeyObjectInspector());
			valueInspector = GenerateInspectorHandle( mInsp.getMapValueObjectInspector());
		}

		@Override
		public String generateJson(Object obj) {
			if( obj == null) {
				return "null";
			}
			StringBuilder builder = new StringBuilder();
			builder.append( " { " );
			Map map = mapInspector.getMap(obj);
			boolean isFirst = true;
			Iterator<Map.Entry> iter = map.entrySet().iterator();
			while( iter.hasNext())	 {
				Map.Entry entry = iter.next();
				if( !isFirst) {
					builder.append(",");
				} else {
					isFirst = false;
				}
				String keyJson = keyInspector.generateJson(entry.getKey());
				builder.append(keyJson);
				builder.append(":");
				String valJson = valueInspector.generateJson( entry.getValue() );
				builder.append(valJson);
			}
			builder.append(" } ");
			return builder.toString();	
		}

	}
	
	
	private class StructInspectorHandle implements InspectorHandle {
		private StructObjectInspector structInspector;
		private List<String> fieldNames;
		private List<InspectorHandle> fieldInspectorHandles;
		
		public StructInspectorHandle(StructObjectInspector insp) throws UDFArgumentException {
			structInspector = insp;
			List<? extends StructField> fieldList = insp.getAllStructFieldRefs();
			this.fieldNames = new ArrayList<String>();
			this.fieldInspectorHandles = new ArrayList<InspectorHandle>();
			for(StructField sf : fieldList) {
			   fieldNames.add( sf.getFieldName());
			   fieldInspectorHandles.add( GenerateInspectorHandle( sf.getFieldObjectInspector() ));
			}
		}

		@Override
		public String generateJson(Object obj) {
			if(obj== null) {
				return null;
			}
		    //// Interpret a struct as a map ...	
			StringBuilder sb = new StringBuilder();
			sb.append(" { ");
		    List structObjs = structInspector.getStructFieldsDataAsList(obj);
			
		    boolean isFirst = true;
			for(int i=0; i<fieldNames.size(); ++i) {
				if(!isFirst) {
					sb.append(",");
				} else {
					isFirst = false;
				}
				sb.append("\"");
				sb.append( fieldNames.get(i));
				sb.append("\"");
				sb.append(":");
				sb.append( fieldInspectorHandles.get(i).generateJson( structObjs.get(i)));
			}
		    sb.append(" } ");
		    return sb.toString();
		}
		
	}


	private class ArrayInspectorHandle implements InspectorHandle {
		private ListObjectInspector arrayInspector;
		private InspectorHandle valueInspector;


		public ArrayInspectorHandle( ListObjectInspector lInsp) throws UDFArgumentException {
			arrayInspector = lInsp;
			valueInspector = GenerateInspectorHandle( arrayInspector.getListElementObjectInspector());
		}

		@Override
		public String generateJson(Object obj) {
			if( obj == null) {
				return "null";
			}
			StringBuilder builder = new StringBuilder();
			builder.append( " [ " );
			List list = arrayInspector.getList( obj);
			boolean isFirst = true;
			for( Object listObj : list)	 {
				if( !isFirst) 
					builder.append(",");
				else
				    isFirst = false;
				String listObjJson = valueInspector.generateJson(listObj);
				builder.append(listObjJson);
			}
			builder.append(" ] ");
			return builder.toString();	
		}

	}

	private class StringInspectorHandle implements InspectorHandle {
		private StringObjectInspector strInspector;


		public StringInspectorHandle( StringObjectInspector insp) {
			strInspector = insp;
		}

		@Override
		public String generateJson(Object obj) {
			if( obj == null) {
				return "null";
			}
			StringBuilder builder = new StringBuilder();
			builder.append( "\"" );
			String str = strInspector.getPrimitiveJavaObject(obj);
			//// TODO  escape the strings ????  XXX
			builder.append( str);
			builder.append( "\"");
			return builder.toString();
		}

	}

	private class IntInspectorHandle implements InspectorHandle {
		private IntObjectInspector intInspector;

		public IntInspectorHandle( IntObjectInspector insp) {
			intInspector = insp;
		}
		@Override
		public String generateJson(Object obj) {
			if( obj == null) {
				return "null";
			}
			int num = intInspector.get(obj);
			return Integer.toString(num);
		}
	}
	
	private class DoubleInspectorHandle implements InspectorHandle {
		private DoubleObjectInspector dblInspector;

		public DoubleInspectorHandle( DoubleObjectInspector insp) {
			dblInspector = insp;
		}
		@Override
		public String generateJson(Object obj) {
			if( obj == null) {
				return "null";
			}
			double num = dblInspector.get(obj);
			return Double.toString(num);
		}
	}

	private class LongInspectorHandle implements InspectorHandle {
		private LongObjectInspector longInspector;

		public LongInspectorHandle( LongObjectInspector insp) {
			longInspector = insp;
		}
		@Override
		public String generateJson(Object obj) {
			if( obj == null) {
				return "null";
			}
			long num = longInspector.get(obj);
			return Long.toString(num);
		}
	}

	private class BooleanInspectorHandle implements InspectorHandle {
		private BooleanObjectInspector boolInspector;

		public BooleanInspectorHandle( BooleanObjectInspector insp) {
			boolInspector = insp;
		}
		@Override
		public String generateJson(Object obj) {
			if( obj == null) {
				return "null";
			}
			boolean tf = boolInspector.get( obj);
			return Boolean.toString(tf);
		}
	}

	private  InspectorHandle GenerateInspectorHandle( ObjectInspector insp) throws UDFArgumentException {
		Category cat = insp.getCategory();
		if( cat == Category.MAP) {
			return new MapInspectorHandle((MapObjectInspector) insp);
		} else if( cat == Category.LIST ) {
			return new ArrayInspectorHandle( (ListObjectInspector) insp);
		} else if( cat == Category.STRUCT ) {
			return new StructInspectorHandle( (StructObjectInspector) insp);
		} else if( cat == Category.PRIMITIVE ) {
			PrimitiveObjectInspector primInsp = (PrimitiveObjectInspector) insp;
			PrimitiveCategory primCat = primInsp.getPrimitiveCategory();
			if( primCat == PrimitiveCategory.STRING) {
				return new StringInspectorHandle((StringObjectInspector) primInsp);
			} else if( primCat == PrimitiveCategory.INT ) {
				return new IntInspectorHandle((IntObjectInspector) primInsp);
			} else if( primCat == PrimitiveCategory.LONG ) {
				return new LongInspectorHandle((LongObjectInspector) primInsp);
			} else if( primCat == PrimitiveCategory.BOOLEAN) {
				return new BooleanInspectorHandle((BooleanObjectInspector) primInsp);
			} else if( primCat == PrimitiveCategory.DOUBLE) {
				return new DoubleInspectorHandle((DoubleObjectInspector) primInsp);
			}


		}
		/// Dunno ... 
	  throw new UDFArgumentException("Don't know how to handle object inspector " + insp);
  }
	


  @Override
  public Object evaluate(DeferredObject[] args) throws HiveException {
	  return inspHandle.generateJson( args[0].get() );
  }

  @Override
  public String getDisplayString(String[] args) {
    return "to_json(" + args[0] + ")";
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] args)
      throws UDFArgumentException {
    if(args.length != 1 ) {
      throw new UDFArgumentException(" ToJson takes only one object as an argument");
    }
    ObjectInspector oi= args[0];
    inspHandle = GenerateInspectorHandle( oi);

    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }

}
