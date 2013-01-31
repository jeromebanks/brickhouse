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
import java.io.IOException;
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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *  Generate an arbitrary Hive structure from a JSON string,
 *    and an example template object.
 *
 * The UDF takes a JSON string as the first argument, and the second argument defines 
 *   the return type of the UDF, and which fields are parsed from the JSON string.
 *   To parse JSON maps with values of varying types, use struct() to create a structure 
 *    with the desired JSON keys. The template object should be constant.
 *   
 *   For example, 
 *    from_json( " { "name":"Bob","value":23.0,colors["red","yellow","green"],
 *                 "inner_map":{"a":1,"b":2,"c":3 }" , 
 *                 struct("name", "","value", 0.0,"colors", array(""), "inner_map", map("",1) );
 *                       
 *   
 */
@Description(name="from_json",
value = "_FUNC_(a,b) - Returns an arbitrary Hive Structure given a JSON string, and an example template object."
)
public class FromJsonUDF extends GenericUDF {
	private StringObjectInspector jsonInspector;
	private InspectorHandle inspHandle;
	
	private interface InspectorHandle {
		Object parseJson(JsonNode jsonNode);
		
	    ObjectInspector getReturnType();	
	}
	
	/** 
	 * If one passes a named-struct in, then one can parse arbitrary
	 *   structures
	 **/
	private class StructHandle implements InspectorHandle {
		private List<String> fieldNames;
		private List<InspectorHandle> handleList;
		
		
		public StructHandle( StructObjectInspector structInspector) throws UDFArgumentException {
			fieldNames = new ArrayList<String>();
			handleList = new ArrayList<InspectorHandle>();
			
			List<? extends StructField> refs =  structInspector.getAllStructFieldRefs();
			for( StructField ref : refs) {
				fieldNames.add( ref.getFieldName());
				InspectorHandle fieldHandle = generateInspectorHandle( ref.getFieldObjectInspector() );
				handleList.add( fieldHandle);
			}
		}

		@Override
		public Object parseJson(JsonNode jsonNode) {
			/// For structs, they just return a list of object values
			List<Object> valList = new ArrayList<Object>();
			
			for(int i=0; i< fieldNames.size(); ++i) {
				String key = fieldNames.get( i);
				JsonNode valNode = jsonNode.get( key);
				InspectorHandle valHandle = handleList.get(i);
				
				Object valObj = valHandle.parseJson(valNode);
				valList.add( valObj);
			}
			
			return valList;
		}

		@Override
		public ObjectInspector getReturnType() {
			List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
			for( InspectorHandle fieldHandle : handleList) {
				structFieldObjectInspectors.add( fieldHandle.getReturnType() );
			}
			return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, structFieldObjectInspectors);
		}
		
	};
	
	private class MapHandle implements InspectorHandle {
		private InspectorHandle mapValHandle;
		private StandardMapObjectInspector retInspector;

		/// for JSON maps (or "objects"), the keys are always string objects
		///  
		public MapHandle( MapObjectInspector insp) throws UDFArgumentException {
			if( !(insp.getMapKeyObjectInspector() instanceof StringObjectInspector)) {
				throw new RuntimeException( " JSON maps can only have strings as keys");
			}
			mapValHandle = generateInspectorHandle( insp.getMapValueObjectInspector() );
		}
		@Override
		public Object parseJson(JsonNode jsonNode) {
			Map<String,Object> newMap = (Map<String,Object>)retInspector.create();
			
			Iterator<String> keys = jsonNode.getFieldNames();
			while( keys.hasNext()) {
				String key = keys.next();
				JsonNode valNode = jsonNode.get( key);
				Object val = mapValHandle.parseJson(valNode);
				newMap.put( key, val);
			}
			return newMap;
		}

		@Override
		public ObjectInspector getReturnType() {
			retInspector = ObjectInspectorFactory.getStandardMapObjectInspector(
					PrimitiveObjectInspectorFactory.javaStringObjectInspector,
					mapValHandle.getReturnType() );
			return retInspector;
		}
		
	}
	
	private class ListHandle implements InspectorHandle {
		private StandardListObjectInspector retInspector;
		private InspectorHandle elemHandle;

		public ListHandle( ListObjectInspector insp) throws UDFArgumentException {
			elemHandle = generateInspectorHandle( insp.getListElementObjectInspector() );
		}
		
		@Override
		public Object parseJson(JsonNode jsonNode) {
			List newList = (List) retInspector.create(0);
			
			Iterator<JsonNode> listNodes = jsonNode.getElements();
			while(listNodes.hasNext()) {
				JsonNode elemNode = listNodes.next();
				if( elemNode != null) {
					Object elemObj = elemHandle.parseJson(elemNode);
					newList.add( elemObj);
				} else {
					newList.add(null);
				}
			}
			return newList;
		}

		@Override
		public ObjectInspector getReturnType() {
			retInspector =  ObjectInspectorFactory.getStandardListObjectInspector( elemHandle.getReturnType() );
			return retInspector;
		}
		
	}
	
	private class PrimitiveHandle implements InspectorHandle {
		private PrimitiveCategory category;
		
		public PrimitiveHandle(PrimitiveObjectInspector insp) throws UDFArgumentException {
			category = insp.getPrimitiveCategory();
			
		}

		@Override
		public Object parseJson(JsonNode jsonNode) {
			if(jsonNode == null) {
				return null;
			}
			switch( category) {
			case STRING:
				return jsonNode.getTextValue();
			case LONG:
				return jsonNode.getLongValue();
			case INT:
				return jsonNode.getIntValue();
			case DOUBLE:
				return jsonNode.getDoubleValue();
			case BOOLEAN:
				return jsonNode.getBooleanValue();
			}
			return null;
		}

		@Override
		public ObjectInspector getReturnType() {
			return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(category);
		}
		
	}

	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		try {
		   String jsonString = jsonInspector.getPrimitiveJavaObject( arg0[0].get());
		
		    ObjectMapper jacksonParser = new ObjectMapper();
			JsonNode jsonNode = jacksonParser.readTree( jsonString);
			
			return inspHandle.parseJson(jsonNode);
		} catch (JsonProcessingException e) {
			throw new HiveException(e);
		} catch (IOException e) {
			throw new HiveException(e);
		}
	}

	@Override
	public String getDisplayString(String[] arg0) {
		return "from_json( \"" + arg0[0] + "\" , \"" + arg0[1] + "\" )";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0)
			throws UDFArgumentException {
		if( arg0.length != 2) {
		    throw new UDFArgumentException("from_json expects a JSON string and a template object");
		}
		if(arg0[0].getCategory() != Category.PRIMITIVE
				|| ((PrimitiveObjectInspector)arg0[0]).getPrimitiveCategory() != PrimitiveCategory.STRING ) {
		    throw new UDFArgumentException("from_json expects a JSON string and a template object");
		}
		jsonInspector = (StringObjectInspector) arg0[0];
		inspHandle = generateInspectorHandle( arg0[1]);
		
		return inspHandle.getReturnType();
	}
	
	
	private InspectorHandle generateInspectorHandle( ObjectInspector insp) throws UDFArgumentException {
		Category cat = insp.getCategory();
		switch( cat)  {
		case LIST:
			return new ListHandle( (ListObjectInspector)insp );
		case MAP:
			return new MapHandle( (MapObjectInspector)insp);
		case STRUCT:
			return new StructHandle( (StructObjectInspector)insp);
		case PRIMITIVE:
			return new PrimitiveHandle( (PrimitiveObjectInspector)insp);
		}
		return null;
	}
		

}
