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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;
import org.joda.time.*;
import org.joda.time.format.DateTimeFormatter;

/**
 *  Similar to AddDays, but adds a number of hours to strings of the form 
 *   'YYYYMMddHH'
 *     
 *
 */
@Description(name = "add_hours",
value = "_FUNC_(YYYYMMDDHH,num_hours) - Generates a date and hour string which is num_hours different from the passed in date hour ")
public class AddHoursUDF extends UDF {
	private static final DateTimeFormatter YYYYMMDDHH = org.joda.time.format.DateTimeFormat.forPattern("YYYYMMddHH");

	public String evaluate(String dateHourStr, int numHours) {
		DateTime dt = YYYYMMDDHH.parseDateTime(dateHourStr);
		DateTime addedHour = dt.plusHours(numHours);
		String addedHourStr = YYYYMMDDHH.print(addedHour);
		
		return addedHourStr;
	}
}
