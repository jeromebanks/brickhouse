package brickhouse.udf.sanity;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;

public class AssertLessThanUDF extends UDF {
	private static final Logger LOG = Logger.getLogger(AssertLessThanUDF.class);

	
	public String evaluate( Double smaller, Double bigger) {
		if( smaller == null || bigger == null ) {
			LOG.error(" Null values found :: " + smaller + " < " + bigger);
			System.err.println(" Null values found :: " + smaller + " < " + bigger);
			throw new RuntimeException(" Null values found :: " + smaller + " < " + bigger);
		}
		if( !( smaller < bigger ) ) {
			LOG.error(" Assertion Not Met :: ! ( " + smaller + " < " + bigger + " ) ");
			System.err.println(" Assertion Not Met :: ! ( " + smaller + " < " + bigger + " ) ");
			throw new RuntimeException(" Assertion Not Met :: ! ( " + smaller + " < " + bigger + " ) ");
		} else {
			return smaller.toString() + " < " + bigger.toString();
		}
	}
}
