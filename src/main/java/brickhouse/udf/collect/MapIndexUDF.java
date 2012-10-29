package brickhouse.udf.collect;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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
public class MapIndexUDF extends UDF {
	private static final Logger LOG = Logger.getLogger( MapIndexUDF.class);
	
    public Double evaluate( Map<String,Double> map, String key) throws IOException	{
    	/// XXX TODO For now, just assume an array of strings 
    	/// XXX TODO In future, make a GenericUDF, so one can handle multiple types
    	/// XXX
        return map.get(key);
    	
    }

}
