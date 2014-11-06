package brickhouse.udf.xunit;
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
 *
 */


import java.util.ArrayList;
import java.util.IllegalFormatException;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.log4j.Logger;

/**
 *   Generate XUnits for a set of dimensional values
 *  
 *   Users pass in an array of Hive structures representing the 
 *     values 
 */
@Description(
		name="xunit_explode", 
		value="_FUNC_(array<struct<dim:string,attr_names:array<string>,attr_values:array<string>>) - ",
		extended="SELECT _FUNC_(uid, ts), uid, ts, event_type from foo;")
public class XUnitExplodeUDTF extends GenericUDTF {
	private static final Logger LOG = Logger.getLogger( XUnitExplodeUDTF.class);
	
	private static final String GLOBAL_UNIT = "/G";

	private ListObjectInspector listInspector;
	private StructObjectInspector structInspector;
	private StructField dimField;
	private StructField attrNamesField;
	private StructField attrValuesField;
	private ListObjectInspector attrNamesInspector;
	private StringObjectInspector attrNameInspector;
	private ListObjectInspector attrValuesInspector;
	private StringObjectInspector attrValueInspector;
	private String[] xunitFieldArr = new String[1];

	@Override
	public void close() throws HiveException {
		
	}
	
	
	private void usage( String mess) throws UDFArgumentException {
	   LOG.error("Invalid arguments. xunit_explode expects an array of structs containing dimension name and attribute values; " + mess);	
	   throw new UDFArgumentException("Invalid arguments. xunit_explode expects an array of structs containing dimension name and attribute values; " + mess);	
	}

	@Override
	public StructObjectInspector initialize(ObjectInspector[] objInspectorArr)
			throws UDFArgumentException {
		//// Need to make sure that it is our array of structs inspector 
		if(objInspectorArr.length != 1 ) {
		    usage(" Only one argument");
		}
		ObjectInspector objInsp = objInspectorArr[0];
		if(objInsp.getCategory() != Category.LIST) {
		   usage(" Must be a list");
		}
		
		listInspector = (ListObjectInspector) objInsp;
		if(listInspector.getListElementObjectInspector().getCategory() != Category.STRUCT ) {
		   usage(" Must be an array of structs");
		}
		structInspector = (StructObjectInspector) listInspector.getListElementObjectInspector();
		dimField = structInspector.getStructFieldRef("dim");
		if(dimField == null) {
		    usage("Struct must have a 'dim' field");
		}
		if(dimField.getFieldObjectInspector().getCategory() != Category.PRIMITIVE
				|| ((PrimitiveObjectInspector)dimField.getFieldObjectInspector()).getPrimitiveCategory() != PrimitiveCategory.STRING) {
		    usage("dim field must be a string");
		}
		StringObjectInspector dimInspector = (StringObjectInspector) dimField.getFieldObjectInspector();

		attrNamesField = structInspector.getStructFieldRef("attr_names");
		if(attrNamesField == null) {
		    usage("Struct must have a 'attr_names' field");
		}
		if(attrNamesField.getFieldObjectInspector().getCategory() != Category.LIST) {
			usage("attr_names needs to be a list");
		}
		attrNamesInspector = (ListObjectInspector) attrNamesField.getFieldObjectInspector();
		if(attrNamesInspector.getListElementObjectInspector().getCategory() != Category.PRIMITIVE
				|| ((PrimitiveObjectInspector)attrNamesInspector.getListElementObjectInspector()).getPrimitiveCategory() != PrimitiveCategory.STRING) {
		    usage("attr_names needs to be a list of strings");
		}
		attrNameInspector = (StringObjectInspector) attrNamesInspector.getListElementObjectInspector();

		attrValuesField = structInspector.getStructFieldRef("attr_values");
		if(attrValuesField == null) {
		    usage("Struct must have a 'attr_values' field");
		}
		if(attrValuesField.getFieldObjectInspector().getCategory() != Category.LIST) {
			usage("attr_values needs to be a list");
		}
		attrValuesInspector = (ListObjectInspector) attrValuesField.getFieldObjectInspector();
		if(attrValuesInspector.getListElementObjectInspector().getCategory() != Category.PRIMITIVE
				|| ((PrimitiveObjectInspector)attrValuesInspector.getListElementObjectInspector()).getPrimitiveCategory() != PrimitiveCategory.STRING) {
		    usage("attr_values needs to be a list of strings");
		}
		attrValueInspector = (StringObjectInspector) attrValuesInspector.getListElementObjectInspector();
		
		
		/// We return a struct with one field, 'xunit'
		
		ArrayList<String> fieldNames = new ArrayList<String>();
	    fieldNames.add("xunit");
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
		fieldOIs.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		    
	     return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
		      fieldOIs);
	}
	
	private int totalXUnits = 0;

	@Override
	public void process(Object[] args) throws HiveException {
		List<Object>  dimValuesList = (List<Object>) listInspector.getList(args[0]);

		//// Always process the Global Unit 
		forwardXUnit(GLOBAL_UNIT );

		try {
			if(dimValuesList.size() > 0  ) {

				Object firstStruct = dimValuesList.get(0);
				List<Object> otherStructs = dimValuesList.subList( 1, dimValuesList.size() );
				List<String> allCombos = combinations( firstStruct, otherStructs);

				totalXUnits += allCombos.size();
				LOG.info(" SIZE OF ALL XUNIT COMBINATIONS IS " + allCombos.size() + " TOTAL XUNITS SO FAR IS " + totalXUnits);
				for( String xunit : allCombos ) {
					forwardXUnit( xunit);
				}
			}
		} catch(IllegalArgumentException illArg) {
			LOG.error("Error generating XUnits", illArg);
		}

	}
	
	private void forwardXUnit( String xunit) throws HiveException {
		///LOG.info(" Forwarding XUnit " + xunit);
	    xunitFieldArr[0] = xunit;
	    forward( xunitFieldArr);
	}
	
	
	/**
	 *  Generate the "per-dimension" portion of the xunit
	 *  
	 */
	private String getDimBase( Object structObj) {
		StringBuilder sb = new StringBuilder();
		sb.append("/");
		sb.append(structInspector.getStructFieldData(structObj,dimField));
		
		return sb.toString();
	}

	private List<String> generateYPaths( Object structObj, int level, List<String> prevLevels) throws IllegalArgumentException {
		StringBuilder sb = new StringBuilder();
		sb.append("/");
		
		List nameList = (List) structInspector.getStructFieldData(structObj, attrNamesField);
		List valueList = (List) structInspector.getStructFieldData(structObj, attrValuesField);
		
		if(nameList.size() != valueList.size()) {
			throw new IllegalArgumentException("Number of atttribute names must equal number of attribute values");
		}
	
		List<String> retVal = new ArrayList<String>();
		
		String attrValue = attrValueInspector.getPrimitiveJavaObject(valueList.get(level));
	    String attrName = attrNameInspector.getPrimitiveJavaObject(nameList.get(level));
	    
	    if( attrValue != null) {
	    	for(String prefix : prevLevels) {
	    		if(! attrValue.contains("|")) {
	    			String ypath = prefix + "/" + attrName + "=" + attrValue;
	    			LOG.info(" Adding YPATH " + ypath);
	    			retVal.add(ypath);
	    		} else{
				   ///// If we want to emit multiple rows for an xunit, for a particular YPath
				   ////  (ie. Both Asian and Hispanic ethnicity  )
	    			String[] subVals = attrValue.split("\\|");
	    		    for(String subVal : subVals) {
	    			  String ypath = prefix + "/" + attrName + "=" + subVal;
	    			  LOG.info(" MULTI VALUE " + ypath);
	    			  retVal.add(ypath);
	    		    }
	    		}
	    	}
	    }
	    return retVal;
	}
	
	private List<String> generateAllYPaths( Object structObj) throws IllegalArgumentException {
		List nameList = (List) structInspector.getStructFieldData(structObj, attrNamesField);
		List<String> retVals = new ArrayList<String>();
		List<String> prevLevel= new ArrayList<String>();
		String dimBase = getDimBase( structObj);
		if( nameList.size() > 0) {
		   prevLevel.add( dimBase);
		   for(int i=0; i<nameList.size(); ++i ) {
			   List<String> nextLevel = generateYPaths( structObj, i,prevLevel);
			   retVals.addAll( nextLevel);
			   prevLevel = nextLevel;
		   }
		}
		LOG.info(" ALL YPATHS for Struct OBJ for DIM " + dimBase + " = " + retVals.size());
	    return retVals;	
	}
	
	///
	/// XXX JDB 
	/// For now, do brute force recursive method 
	///  Later, try to reuse objects .. ( ie common geo or gender units don't need to be rebuilt)
	////    to reduce garbage collection 
	//// Also allow to specify some pruning logic
	private List<String> combinations( Object structObj,  List<Object>  otherDims) {
		List<String> thisYUnits = generateAllYPaths( structObj);
		if( otherDims.size() == 0 ) {
			return thisYUnits;
		} else {
			Object nextObj = otherDims.get(0);
			List<Object> nextList = otherDims.subList(1, otherDims.size());
		    List<String> otherYUnits = combinations(nextObj, nextList);
		    
		    List<String> allCombos = new ArrayList();
		    //// XXX XXX 
		    /// XXX Add ability to specify pruning logic as argument to UDF
		    allCombos.addAll( thisYUnits);
		    allCombos.addAll( otherYUnits);
		    for( String thisYUnit :  thisYUnits ) {
		        for(String otherYUnit : otherYUnits ) {
		        	allCombos.add( thisYUnit + "," + otherYUnit);
		        }
		    }
		    return allCombos;
		}
	}

}
