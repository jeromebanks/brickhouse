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
 *  Figures out the day number in the calander year of a given date string in YYYYMMDD format
 *   'YYYYMMddHH'
 *
 *
 */
@Description(name = "dayofyear",
        value = "_FUNC_(YYYYMMDD) - Generates number of calander days (1-365) for a given date string ")
public class DayOfYearUDF extends UDF {
    private static final DateTimeFormatter YYYYMMDD = org.joda.time.format.DateTimeFormat.forPattern("YYYYMMdd");

    public static int evaluate(String dateStr) {
        DateTime dt = YYYYMMDD.parseDateTime(dateStr);
        return dt.getDayOfYear();
    }
}
