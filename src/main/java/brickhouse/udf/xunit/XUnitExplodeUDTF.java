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


import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import brickhouse.udf.counter.IncrCounterUDF;

/**
 *   Generate XUnits for a set of dimensional values
 *  
 *   Users pass in an array of Hive structures representing the 
 *     values 
 */
@Description(
		name="xunit_explode", 
		value="_FUNC_(array<struct<dim:string,attr_names:array<string>,attr_values:array<string>>, int, boolean) - ",
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
	
	private IntObjectInspector maxDimInspector = null;
    private int maxDims = -1;
    
    private BooleanObjectInspector globalFlagInspector;
    
    private Reporter reporter;
    
    private Reporter getReporter() throws ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    	if(reporter == null) {
    		reporter = IncrCounterUDF.GetReporter();
    	}
    	return reporter;
    }
    
    
    private void incrCounter( String counterName, long counter) {
       try {
		 getReporter().incrCounter("XUnitExplode", counterName, counter);
	   } catch ( Exception exc) {
		   LOG.error("Error incrementing counter " + counterName, exc);
	   }	
    }

	@Override
	public void close() throws HiveException {
		
	}
	
	
	public XUnitDesc fromPath(YPathDesc yp ) {
	  return new XUnitDesc( yp);
	}
	
	public XUnitDesc addYPath(XUnitDesc xunit , YPathDesc yp) {
	   return xunit.addYPath( yp);
	}
	
	public YPathDesc appendAttribute( YPathDesc yp, String attrName, String attrValue) {
	   return yp.addAttribute(attrName, attrValue);
	}
	
	/**
	 *  For now, just check that number of dimensions doesn't exceed our max.
	 *  In the future, it could be more complicated logic,
	 *   like including only certain YPaths together...
	 *    
	 * @param xunit
	 * @return
	 */
	public boolean shouldIncludeXUnit(XUnitDesc xunit ) {
	   return xunit.numDims() <= maxDims;	
	}
	
	private void usage( String mess) throws UDFArgumentException {
	   LOG.error("Invalid arguments. xunit_explode expects an array of structs containing dimension name and attribute values; " + mess);	
	   throw new UDFArgumentException("Invalid arguments. xunit_explode expects an array of structs containing dimension name and attribute values; " + mess);	
	}

	@Override
	public StructObjectInspector initialize(ObjectInspector[] objInspectorArr)
			throws UDFArgumentException {
		//// Need to make sure that it is our array of structs inspector 
		if(objInspectorArr.length > 3 ) {
		    usage(" Only one,two or three arguments");
		}
		ObjectInspector objInsp = objInspectorArr[0];
		if(objInsp.getCategory() != Category.LIST) {
		   usage(" First arg must be a list");
		}
		
		if( objInspectorArr.length >1 ) {
			if( !(objInspectorArr[1] instanceof IntObjectInspector) ) {
		       usage(" Number of dimensions must be a constant integer.");
			}
			maxDimInspector = (IntObjectInspector) objInspectorArr[1];
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
		
		
		if(objInspectorArr.length > 2) {
		   if(objInspectorArr[2].getCategory() != Category.PRIMITIVE
				   || (((PrimitiveObjectInspector)objInspectorArr[2]).getPrimitiveCategory() != PrimitiveCategory.BOOLEAN)) {
			   usage(" Explode Global flag must be a boolean");
		   }
		   globalFlagInspector = (BooleanObjectInspector) objInspectorArr[2];
		}
		
		/// We return a struct with one field, 'xunit'
		
		ArrayList<String> fieldNames = new ArrayList<String>();
	    fieldNames.add("xunit");
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
		fieldOIs.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		    
	     return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
		      fieldOIs);
	}
	
	@Override
	public void process(Object[] args) throws HiveException {
		List<Object>  dimValuesList = (List<Object>) listInspector.getList(args[0]);

		if( globalFlagInspector != null ) {
			boolean globalFlag = globalFlagInspector.get( args[2]);
			if(globalFlag) {
		      forwardXUnit(GLOBAL_UNIT );
			}
		} else {
		  forwardXUnit(GLOBAL_UNIT );
		}

		if(maxDimInspector != null) {
			maxDims = maxDimInspector.get( args[1]);
		}

		try {
			if(dimValuesList.size() > 0  ) {

				Object firstStruct = dimValuesList.get(0);
				List<Object> otherStructs = dimValuesList.subList( 1, dimValuesList.size() );
				List<XUnitDesc> allCombos = combinations( firstStruct, otherStructs);

				
				for( XUnitDesc xunit : allCombos ) {
					   forwardXUnit( xunit.toString());
				}
		        incrCounter("NumXUnits", allCombos.size() );
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
	private YPathDesc getDimBase( Object structObj) {
		return new YPathDesc( structInspector.getStructFieldData(structObj,dimField).toString() );
	}
	
	/**
	 *  Clean out any special characters in the string
	 */
	private String clean( String attrVal) {
		String trimVal = attrVal.trim();
		if(trimVal.length() == 0) {
			return null;
		}
		if(trimVal.toLowerCase().equals("null")) {
			return null;
		}
		String noSlash = trimVal.replace( '/',' ');
		
		return noSlash;
	}

	private List<YPathDesc> generateYPaths( Object structObj) throws IllegalArgumentException {
		List nameList = (List) structInspector.getStructFieldData(structObj, attrNamesField);
		List valueList = (List) structInspector.getStructFieldData(structObj, attrValuesField);
		
		List<YPathDesc> retVal = new ArrayList<YPathDesc>();
		if( nameList == null || valueList == null) {
			return retVal;
		}
		
		if(nameList.size() != valueList.size()) {
			throw new IllegalArgumentException("Number of atttribute names must equal number of attribute values");
		}
	
	    List<YPathDesc> prevYPaths = new ArrayList<YPathDesc>();
	    List<YPathDesc> nextPrevYPaths = new ArrayList<YPathDesc>();
		
		prevYPaths.add( getDimBase(structObj) );
		for(int i=0; i< nameList.size(); ++i) {
		   String attrValue = attrValueInspector.getPrimitiveJavaObject(valueList.get(i));
	       String attrName = attrNameInspector.getPrimitiveJavaObject(nameList.get(i));
	    
	       if(attrValue != null) {
	    	  if(! attrValue.contains("|")) {
	    			String cleanVal = clean( attrValue);
	    			if(cleanVal != null) {
	    				for(YPathDesc prevYPath : prevYPaths) {
	    			       YPathDesc newYp = prevYPath.addAttribute(attrName, cleanVal);
	    			       retVal.add(newYp);
	    			       nextPrevYPaths.add(newYp);
	    				}
	    			}
	    	  } else{
				   ///// If we want to emit multiple rows for an xunit, for a particular YPath
				   ////  (ie. Both Asian and Hispanic ethnicity  )
	    		  //// Assumption is that multiple values will be |-pipe separated ..
	    		  String[] subVals = attrValue.split("\\|");
	    		  for(String subVal : subVals) {
	    			  String cleanSubVal = clean( subVal);
	    			  if( cleanSubVal != null) {
	    				for(YPathDesc prevYPath : prevYPaths) {
	    			       YPathDesc newYp = prevYPath.addAttribute(attrName, cleanSubVal);
	    			       retVal.add(newYp);
	    			       nextPrevYPaths.add( newYp);
	    				}
	    			  }
	    		  }
	    		}
	    	}
	        prevYPaths = nextPrevYPaths;
	        nextPrevYPaths = new ArrayList<YPathDesc>();
	    }
	    return retVal;
	}
	
	private List<XUnitDesc> combinations( Object structObj,  List<Object>  otherDims) {
	    List<XUnitDesc> allCombos = new ArrayList<XUnitDesc>();
		List<YPathDesc> thisYPaths = generateYPaths( structObj);
		if( otherDims.size() == 0 ) {
		    for( YPathDesc thisYP : thisYPaths) {
		       XUnitDesc thisXUnit = fromPath( thisYP);
		       if(shouldIncludeXUnit( thisXUnit)) {
		         allCombos.add(thisXUnit);
		       }
		    }
			return allCombos;
		} else {
			Object nextObj = otherDims.get(0);
			List<Object> nextList = otherDims.subList(1, otherDims.size());
		    List<XUnitDesc> otherXUnits = combinations(nextObj, nextList);
		    
		    for( YPathDesc thisYP : thisYPaths) {
		       XUnitDesc thisXUnit = fromPath( thisYP);
		       if(shouldIncludeXUnit( thisXUnit)) {
		          allCombos.add(thisXUnit);
		       }
		    }

		    allCombos.addAll( otherXUnits);

		    for( YPathDesc thisYPath :  thisYPaths ) {
		        for(XUnitDesc otherXUnit : otherXUnits ) {
		        	XUnitDesc newXUnit = otherXUnit.addYPath(thisYPath);
		        	if( shouldIncludeXUnit(newXUnit)) {
		        	   allCombos.add( newXUnit);
		        	}
		        }
		    }
	        return allCombos;
		}
	}

}
