package brickhouse.udf.xunit;

import java.util.Arrays;

//// XXX Create Class which models the XUnit 
/// Make them immutable, like strings,
/// So we can build them up from previous
public class XUnitDesc {
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
       Arrays.sort(newYps, new YPathDescComparator());
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

    public YPathDesc[] getYPathDesc() {
        return this._ypaths;
    }
	
}