package brickhouse.udf.collect;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;


/**
 *   Workaround for the Hive bug 
 *   https://issues.apache.org/jira/browse/HIVE-1955
 *   
 *  FAILED: Error in semantic analysis: Line 4:3 Non-constant expressions for array indexes not supported key
 *  
 *   
 *  Use instead of [ ] syntax,   
 *
 * @author jeromebanks
 *
 */
public class ArrayIndexUDF extends UDF {
	private static final Logger LOG = Logger.getLogger( ArrayIndexUDF.class);
	
    public String evaluate( List<String> array, int idx) throws IOException	{
    	/// XXX TODO For now, just assume an array of strings 
    	/// XXX TODO In future, make a GenericUDF, so one can handle multiple types
    	///
    	try {
    	  return array.get( idx);
    	} catch(IndexOutOfBoundsException outOfBounds) {
    		LOG.error(" Error trying to get index " + idx + " out of array of size " + array.size());
    		throw new IOException( outOfBounds);
    	}
    	
    }

}
