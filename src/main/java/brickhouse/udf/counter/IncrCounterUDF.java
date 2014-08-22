package brickhouse.udf.counter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.conf.Configuration;

/**
 *  Increment Hadoop counters from a UDF.
 *  
 *  Useful for saving results of a calculation in Hadoop Counters,
 *   which can then be checked for validation,
 *  
 *   Or for tracking progress of a Hive query,based on number of 
 *    rows of certain types being processed, to see the 
 *    relative distribution of the rows being processed.
 * 
 *    
 *    For example
 *    
 *    INSERT OVERWRITE mytable 
 *    SELECT rowGrouping, increment_counter( "My Table Row Groupings", rowGrouping, count(*) ) as rowGroupingCount
 *      FROM otherTable
 *      GROUP BY rowGrouping;
 *      
 *   
 *   Or 
 *   SELECT 
 *      ....
 *     FROM
 *       (  SELECT id, rowGrouping, increment_counter( "My Table Row Groupings", rowGrouping, 1 )
 *            FROM otherTable ) cntr
 *    .... 
 *
 */
@Description(
		name="increment_counter", 
		value="_FUNC_(string, string, int) - increments the Hadoop counter by specified increment and returns the updated value",
		extended="SELECT _FUNC_( counterFamily, counter, count(*) ) FROM mytable GROUP BY counterFamily, counter;")
public class IncrCounterUDF extends UDF {
	private Reporter reporter;

	
	public Long evaluate( String counterFamily, String counter, int increment) throws HiveException {
		try {
		  Reporter reporter = getReporter();
		  reporter.incrCounter( counterFamily, counter, increment);
		  return reporter.getCounter( counterFamily, counter).getValue();
		} catch(Exception exc) {
		   throw new HiveException("Error while accessing Hadoop Counters", exc);
		}
	}
	
	/**
	 *  Reporter can be accessed from the
	 *   Hive MapredContext, but that 
	 *   is not available at compile time.
	 *   
	 *   Use reflection to access it at runtime
	 * @throws ClassNotFoundException 
	 * @throws SecurityException 
	 * @throws NoSuchMethodException 
	 * @throws InvocationTargetException 
	 * @throws IllegalArgumentException 
	 * @throws IllegalAccessException 
	 */
	private Reporter getReporter() throws ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
	  if(reporter == null) {
	      Class clazz = Class.forName("org.apache.hadoop.hive.ql.exec.MapredContext");
	      Method staticGetMethod = clazz.getMethod("get");
	      Object mapredObj = staticGetMethod.invoke(null);
	      Class mapredClazz = mapredObj.getClass();
	      Method getReporter = mapredClazz.getMethod("getReporter");
	      Object reporterObj=  getReporter.invoke( mapredObj);

	      reporter = (Reporter)reporterObj;
	  }
	  return reporter;
	}
}
