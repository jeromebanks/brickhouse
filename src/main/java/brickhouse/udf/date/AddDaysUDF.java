package brickhouse.udf.date;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;

import org.joda.time.*;
import org.joda.time.format.DateTimeFormatter;

/**
 *   Simple date add UDF.
 *   Would use the Hive standard function, but that assumes a 
 *     date format of "YYYY-MM-DD", and we would prefer "YYYYMMDD"
 *     and it is too awkward to include lots of substring functions in our hive
 *     
 * @author jeromebanks
 *
 */
public class AddDaysUDF extends UDF {
	private static final Logger LOG = Logger.getLogger( AddDaysUDF.class);
	private static final DateTimeFormatter YYYYMMDD = org.joda.time.format.DateTimeFormat.forPattern("YYYYMMdd");

	public String evaluate(String dateStr, int numDays) {
		DateTime dt = YYYYMMDD.parseDateTime(dateStr);
		DateTime addedDt = dt.plusDays(numDays);
		String addedDtStr = YYYYMMDD.print(addedDt);
		
		return addedDtStr;
	}
}
