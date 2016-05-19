package brickhouse.udf.xunit;

public class YPathDesc implements Comparable{
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

    public String[] getAttrNames() {
        return _attrNames;
    }

    public String[] getAttrValues() {
        return _attrValues;
    }

    public String getDimName() {
        return this._dimName;
    }

    public int compareTo(Object obj) {
        YPathDesc yp = (YPathDesc)obj;
        return yp.getDimName().compareTo(_dimName);
    }
}