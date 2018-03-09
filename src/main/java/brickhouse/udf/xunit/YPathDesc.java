package brickhouse.udf.xunit;

import java.util.ArrayList;
import java.util.List;

public class YPathDesc {
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

	public String getDimName() { return _dimName; }

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

	public String getAttributeValue( String attrName) {
		for(int i=0; i<_attrNames.length; ++i) {
			if( _attrNames[i].equals( attrName)) {
			  return _attrValues[i];
			}
		}
		return null;
	}

	public String[] getAttributeNames()  { return _attrNames;  }
	public String[] getAttributeValues() { return _attrValues; }

	public int numAttributes() { return _attrNames.length; }


    public String toString() {
       StringBuilder sb = new StringBuilder("/");	
       sb.append( _dimName);
       for(int i=0; i<_attrNames.length; ++i) {
    	  sb.append('/');
    	  sb.append(_attrNames[i]);
    	  sb.append('=');
    	  sb.append(_attrValues[i]); //// XXX Convert slashes and commas
       }
       return sb.toString();
    }


    static public YPathDesc ParseYPath( String ypathStr) throws IllegalArgumentException {
       if( ypathStr.startsWith("/")) {
         String[] splitAttrs = ypathStr.substring(1).split("/");
		 String ypDim = splitAttrs[0];
         if(splitAttrs.length > 1) {
			 ArrayList<String> attrNames = new ArrayList<String>();
			 ArrayList<String> attrValues = new ArrayList<String>();
			 for (int i = 1; i <= splitAttrs.length - 1; ++i) {
				 String[] kvArr = splitAttrs[i].split("=");
				 if (kvArr.length != 2) {
					 throw new IllegalArgumentException(" Invalid YPath "  + ypathStr);
				 }
				 attrNames.add( kvArr[0]);
				 attrValues.add( kvArr[1]);
			 }
			 return new YPathDesc( ypDim, attrNames.toArray( new String[attrNames.size()]), attrValues.toArray( new String[attrValues.size()]));
		 } else {
            throw new IllegalArgumentException(" Invalid YPath " + ypathStr + " ; YPaths must have attribute names and values ");
         }
	   } else {
       	  throw new IllegalArgumentException(" Invalid YPath " + ypathStr + " ; YPaths must start with / ");
	   }
	}
}