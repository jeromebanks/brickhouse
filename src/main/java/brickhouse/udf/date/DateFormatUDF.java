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

import java.util.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;
import org.joda.time.*;
import org.joda.time.format.DateTimeFormatter;

/**
 *  
 *  Arrrrggghhh ... 
 *    Implement our own date_format UDF
 *     
 *
 */
@Description(name = "date_format",
value = "_FUNC_(date,fromFormatString,toFormatString) - Formats a Hive Date according to a Joda pattern string ")
public class DateFormatUDF extends UDF {
	////private static final DateTimeFormatter YYYYMMDDHH = org.joda.time.format.DateTimeFormat.forPattern("YYYYMMddHH");

	public String evaluate(String dt, String fromFormatString, String toFormatString) {
		DateTimeFormatter fromFormatter = org.joda.time.format.DateTimeFormat.forPattern(fromFormatString);
		DateTimeFormatter toFormatter = org.joda.time.format.DateTimeFormat.forPattern(toFormatString);
				
		DateTime fromDt = fromFormatter.parseDateTime(dt);
	    String formatted = toFormatter.print( fromDt);
		return formatted;
	}
}
