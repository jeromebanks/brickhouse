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
import java.util.HashSet;
import java.util.IllegalFormatException;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
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
	
	private IntObjectInspector maxDimInspector = null;
    private int maxDims = -1;

	@Override
	public void close() throws HiveException {
		
	}
	
	
	//// XXX Create Class which models the XUnit 
	/// Make them immutable, like strings,
	/// So we can build them up from previous
	private static class XUnitDesc {
		private YPathDesc[] _ypaths;
		
		public XUnitDesc( YPathDesc yp) {
	       _ypaths = new YPathDesc[]{ yp };
		}
		public XUnitDesc( YPathDesc[] yps) {
	       _ypaths = yps;
		}
		
		public XUnitDesc addYPath(YPathDesc yp) {
		   YPathDesc[] newYps = new YPathDesc[ _ypaths.length + 1];
		   //// Prepend the YPath ..
		   newYps[0] = yp;
		   for(int i=1; i<newYps.length; ++i) {
			  newYps[i] = _ypaths[i -1];
		   }
		   return new XUnitDesc( newYps);
		}
		
		public int numDims() { return _ypaths.length; }
		
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append( _ypaths[0].toString() );
			for(int i=1; i<_ypaths.length; ++i) {
		       sb.append(',');
		       sb.append( _ypaths[i].toString() );
			}
			return sb.toString();
		}
		
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
	
	private static class YPathDesc {
		private String _dimName;
	    private String[] _attrNames;
	    private String[] _attrValues;
		
		public YPathDesc(String dimName) {
			_dimName = dimName;
			_attrNames = new String[0];
			_attrValues = new String[0];
		}
		public YPathDesc( String dimName, String[] attrNames, String[] attrValues) {
		   _dimName = dimName;
	       _attrNames = attrNames;
	       _attrValues = attrValues;
		}
		
		public int numLevels() { return _attrNames.length; }

		public YPathDesc addAttribute( String attrName, String attrValue) {
			String[] newAttrNames = new String[ _attrNames.length + 1];
			String[] newAttrValues = new String[ _attrValues.length + 1];
			for(int i=0; i<_attrNames.length; ++i) {
			  newAttrNames[i] = _attrNames[i];
			  newAttrValues[i] = _attrValues[i];
			}
			newAttrNames[ _attrNames.length] = attrName;
			newAttrValues[ _attrNames.length] = attrValue;
		    return new YPathDesc( _dimName, newAttrNames, newAttrValues);
		}
	    
	    public String toString() {
	       StringBuilder sb = new StringBuilder("/");	
	       sb.append( _dimName);
	       for(int i=0; i<_attrNames.length; ++i) {
	    	  sb.append('/');
	    	  sb.append(_attrNames[i]);
	    	  sb.append('=');
	    	  sb.append(_attrValues[i]);
	       }
	       return sb.toString();
	    }
	    
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
		if(objInspectorArr.length > 2 ) {
		    usage(" Only one or two arguments");
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
		if(maxDimInspector != null) {
			maxDims = maxDimInspector.get( args[1]);
		}

		try {
			if(dimValuesList.size() > 0  ) {

				Object firstStruct = dimValuesList.get(0);
				List<Object> otherStructs = dimValuesList.subList( 1, dimValuesList.size() );
				List<XUnitDesc> allCombos = combinations( firstStruct, otherStructs);

				totalXUnits += allCombos.size();
				
				for( XUnitDesc xunit : allCombos ) {
					   forwardXUnit( xunit.toString());
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
		StringBuilder sb = new StringBuilder();
		sb.append("/");
		
		List nameList = (List) structInspector.getStructFieldData(structObj, attrNamesField);
		List valueList = (List) structInspector.getStructFieldData(structObj, attrValuesField);
		
		if( nameList == null || valueList == null) {
			return null;
		}
		
		if(nameList.size() != valueList.size()) {
			throw new IllegalArgumentException("Number of atttribute names must equal number of attribute values");
		}
	
		List<YPathDesc> retVal = new ArrayList<YPathDesc>();
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
	    			       YPathDesc newYp = prevYPath.addAttribute(attrName, attrValue);
	    			       retVal.add(newYp);
	    			       nextPrevYPaths.add(newYp);
	    				}
	    			}
	    	  } else{
				   ///// If we want to emit multiple rows for an xunit, for a particular YPath
				   ////  (ie. Both Asian and Hispanic ethnicity  )
	    		  String[] subVals = attrValue.split("\\|");
	    		  for(String subVal : subVals) {
	    			  String cleanSubVal = clean( subVal);
	    			  if( cleanSubVal != null) {
	    				for(YPathDesc prevYPath : prevYPaths) {
	    			       YPathDesc newYp = prevYPath.addAttribute(attrName, attrValue);
	    			       retVal.add(newYp);
	    			       nextPrevYPaths.add( newYp);
	    				}
	    			  }
	    		  }
	    		}
	    	}
	        prevYPaths = nextPrevYPaths;
	    }
	    return retVal;
	}
	
	
	///
	/// XXX JDB 
	/// For now, do brute force recursive method 
	///  Later, try to reuse objects .. ( ie common geo or gender units don't need to be rebuilt)
	////    to reduce garbage collection 
	//// Also allow to specify some pruning logic
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
