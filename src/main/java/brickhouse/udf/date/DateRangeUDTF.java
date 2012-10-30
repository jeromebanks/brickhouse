package brickhouse.udf.date;
/**
 * Copyright 2012 Klout, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * DateRange is a UDTF for generating days 
 *   from start date to end_date, inclusively.
 *   
 *   This might be useful in cases where one needed 
 *    to have a row for every day in the range ...
 *    
 *   select 
 *      t1.id,
 *      t1.date,
 *      coalesce( t2.val, 0.0 ) as val
 *   from
 *     ( select id, rng.date 
 *         from tab1
 *         lateral view( date_range( tab1.start_date, tab1.end_date ) ) rng as date, index 
 *     ) t1
 *    left outer join
 *     ( select val from tab2 ) t2
 *    on ( t1.id = t2.id and t1.date = t2.date );
 *    
 *
 */
@Description(name = "date_range",
value = "_FUNC_(a,b,c) - Generates a range of integers from a to b incremented by c"
  + " or the elements of a map into multiple rows and columns ")
public class DateRangeUDTF extends GenericUDTF {
	private static DateTimeFormatter YYMMDD = DateTimeFormat.forPattern( "YYYYMMdd");
	private StringObjectInspector startInspector = null;
	private StringObjectInspector endInspector = null;
	private IntObjectInspector incrInspector = null;

	@Override
	public StructObjectInspector initialize(ObjectInspector[] argOIs)
			throws UDFArgumentException {
		if( argOIs.length < 2 || argOIs.length > 3) {
			throw new UDFArgumentException("DateRange takes <startdate>, <enddate>, <optional increment>");
		}
		
	    if( ! ( argOIs[0] instanceof StringObjectInspector)	)
			throw new UDFArgumentException("DateRange takes <startdate>, <enddate>, <optional increment>");
	    else 
	    	startInspector = (StringObjectInspector) argOIs[0];
		
	    if( ! ( argOIs[1] instanceof StringObjectInspector)	)
			throw new UDFArgumentException("DateRange takes <startdate>, <enddate>, <optional increment>");
	    else 
	    	endInspector = (StringObjectInspector) argOIs[1];
	    
	    if( argOIs.length == 3) {
	       if( ! ( argOIs[2] instanceof IntObjectInspector)	)
			   throw new UDFArgumentException("DateRange takes <startdate>, <enddate>, <optional increment>");
	       else 
	    	   incrInspector = (IntObjectInspector) argOIs[2];
	    }
		
		
	    ArrayList<String> fieldNames = new ArrayList<String>();
	    fieldNames.add("date");
	    fieldNames.add("index");
	    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
	    fieldOIs.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector);
	    fieldOIs.add( PrimitiveObjectInspectorFactory.javaIntObjectInspector);
	    
	    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
	        fieldOIs);
	}

    private final Object[] forwardListObj = new Object[2];
    
	@Override
	public void process(Object[] args) throws HiveException {
		String start = null; 
		String end = null; 
		int incr = 1;
		switch( args.length) {
		case 3:
			incr = incrInspector.get(args[2]);
		case 2:
			start = startInspector.getPrimitiveJavaObject( args[0]);
			end = endInspector.getPrimitiveJavaObject( args[1]);
			break;
		}
		try {
	       DateTime startDt = YYMMDD.parseDateTime( start);
	       DateTime endDt = YYMMDD.parseDateTime( end);
	       int i=0;
	       for(DateTime dt= startDt; dt.isBefore( endDt) || dt.isEqual( endDt); dt = dt.plusDays( incr),i++) {
	    	   forwardListObj[0] = YYMMDD.print( dt);
	    	   forwardListObj[1] = new Integer(i);
	    	   
	    	   forward( forwardListObj);
	       }
		} catch(IllegalArgumentException badFormat) {
			throw new HiveException("Unable to parse dates; start = " + start + " ; end = " + end);
		}
	}

	@Override
	public void close() throws HiveException {
		
	}

}
