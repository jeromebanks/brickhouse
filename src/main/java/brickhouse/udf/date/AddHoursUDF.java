package brickhouse.udf.date;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class AddHoursUDF extends UDF
{

    public AddHoursUDF()
    {
    }

    public String evaluate(String dateHourStr, int numHours)
    {
        DateTime dt = YYYYMMDDHH.parseDateTime(dateHourStr);
        DateTime addedHour = dt.plusHours(numHours);
        String addedHourStr = YYYYMMDDHH.print(addedHour);
        return addedHourStr;
    }

    private static final DateTimeFormatter YYYYMMDDHH = DateTimeFormat.forPattern("YYYYMMddHH");

}