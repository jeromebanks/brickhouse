package brickhouse.udf.date;

/**
 * Copyright 2015 Klout, Inc
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

import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.PeriodFormatter;

/**
 * UDF that receives date, date format (such as "YYYY-MM-dd HH:mm:ss") and
 * period in ISO8601, it adds period in seconds to date.
 *
 * Example: add_iso_period(date, 'YYYY-MM-dd HH:mm:ss', 'PT02H00M')
 */
public class AddISOPeriodUDF extends UDF {

	private static final PeriodFormatter periodFormatter = org.joda.time.format.ISOPeriodFormat
			.standard();

	public String evaluate(String dateString, String dateFormat, String periodString) {
		if (dateString == null) {
			return null;
		}

		if (dateFormat == null) {
			dateFormat = "YYYY-MM-dd HH:mm:ss";
		}

		DateTimeFormatter dateFormatter = org.joda.time.format.DateTimeFormat.forPattern(dateFormat);
		DateTime input = dateFormatter.parseDateTime(dateString);

		Duration duration = periodFormatter.parsePeriod(periodString).toStandardDuration();
		long seconds = duration.getStandardSeconds();

		DateTime output = input.plusSeconds(Long.valueOf(seconds).intValue());
		return dateFormatter.print(output);
	}
}
