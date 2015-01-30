package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 *  Return an XUnit with a specific 
 *    YPath dimension removed.
 *
 */
public class RemoveYPathUDF extends UDF {
	
	   public String evaluate( String ypathPrefix, String xunit) {
	        String[] ypaths = xunit.split(",");
	        StringBuilder sb = new StringBuilder();
	        
	        for( int i=0; i<ypaths.length; ++i) {
	        	String ypath = ypaths[i];
	        	if( ! ypath.startsWith(ypathPrefix)) {
	        	   if( sb.length() > 0)	 {
	        		   sb.append(",");
	        	   }
	        	   sb.append(ypath);
	        	}
	        }
	        return sb.toString();	
	    }

}
