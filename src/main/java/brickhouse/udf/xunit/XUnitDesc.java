package brickhouse.udf.xunit;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 *   XUnitDesc is lightweight Java class
 *     to simply model an XUnit within Brickhouse.
 *
 */
public class XUnitDesc {
	private YPathDesc[] _ypaths;
	
	public XUnitDesc( YPathDesc yp) {
       _ypaths = new YPathDesc[]{ yp };
	}
	public XUnitDesc( YPathDesc[] yps) {
       _ypaths = yps;
	}

	/// XXX ??? Where is this called ??
    /// How do we ensure proper ordering ?
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
	public YPathDesc[] getYPaths() { return _ypaths; }

	public boolean isGlobal() {
		if(_ypaths.length == 1) {
		  return _ypaths[0].isGlobal();
		} else {
			return false;
		}
	}

	public boolean containsDim( String ypDim) {
		for(YPathDesc ypd : _ypaths) {
			if( ypd.getDimName().equals( ypDim)) {
				return true;
			}
		}
		return false;
	}


    Comparator<YPathDesc> yPathComparator = new Comparator<YPathDesc>() {
        @Override
        public int compare(YPathDesc o1, YPathDesc o2) {
            return o1.getDimName().compareTo(o2.getDimName());
        }
    };

	public XUnitDesc appendYPath( YPathDesc yp) {
	  //// Want to keep YPaths in sorted order
        if( ! isGlobal()) {
            YPathDesc[] newYPaths = Arrays.copyOf(_ypaths, _ypaths.length + 1);
            newYPaths[newYPaths.length - 1] = yp;

            Arrays.sort(newYPaths, yPathComparator );
            return new XUnitDesc( newYPaths);
        } else {
            return new XUnitDesc( yp);
        }
	}

	public XUnitDesc removeYPath( String ypDim) {
	    if( isGlobal()) {
	      return this;
		} else {
	      List<YPathDesc> filteredYPaths = new ArrayList<YPathDesc>();
	      for(int i=0; i< _ypaths.length -1; ++i) {
	        YPathDesc yp = _ypaths[i];
	        if( ! yp.getDimName().equals( ypDim)) {
	        	filteredYPaths.add( yp);
			}
		  }
		  if( filteredYPaths.size() > 0) {
	        return new XUnitDesc(( filteredYPaths.toArray( new YPathDesc[filteredYPaths.size()])));
		  } else {
	      	return XUnitDesc.GlobalXUnit;
		  }
	    }
	}

	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append( _ypaths[0].toString() );
		for(int i=1; i<_ypaths.length; ++i) {
	       sb.append(',');
	       sb.append( _ypaths[i].toString() );
		}
		return sb.toString();
	}

	public static XUnitDesc GlobalXUnit = new XUnitDesc( YPathDesc.GlobalYPath);
	public static String GlobalXUnitString = GlobalXUnit.toString();

	static public XUnitDesc ParseXUnit( String xunitStr) throws IllegalArgumentException {
	    if( xunitStr.equals("/G")) {
	        return GlobalXUnit;
		} else {
	      String[] ypathStrArr = xunitStr.split(",");
	      YPathDesc[] ypathArr = new YPathDesc[ypathStrArr.length];
	      for(int i=0; i<ypathStrArr.length -1; ++i) {
	      	ypathArr[i] = YPathDesc.ParseYPath( ypathStrArr[i]);
		  }
		  return new XUnitDesc(ypathArr);
		}
	}
}