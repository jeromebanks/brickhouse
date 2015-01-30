package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 *  Get a particular YPath from 
 *    an XUnit
 *
 */
public class GetYPathUDF extends UDF {
    public String evaluate( String ypathPrefix, String xunit) {
        String[] ypaths = xunit.split(",");
        
        for( int i=0; i<ypaths.length; ++i) {
        	String ypath = ypaths[i];
        	if(  ypath.startsWith(ypathPrefix)) {
        		return ypath;
        	}
        }
        return null;
    }
}
