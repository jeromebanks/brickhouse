package brickhouse.udf.sanity;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;

/**
 *  UDF to throw an error if some assertion is not met.
 *  
 * @author jeromebanks
 *
 */
public class ThrowErrorUDF extends UDF {
	private static final Logger LOG = Logger.getLogger(ThrowErrorUDF.class);
	
	
	public String evaluate(String errorMessage) {
		LOG.error("Assertion not met :: " + errorMessage);
		System.err.println("Assertion not met :: " + errorMessage);
		
		throw new RuntimeException( errorMessage);
	}

}
