package tagged.udf.macros;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.*;
import org.joda.time.format.DateTimeFormatter;

/**
 * Created by janandaram on 10/23/15.
 */
public class HH extends UDF {

    private static final DateTimeFormatter dtFormatter = org.joda.time.format.DateTimeFormat.forPattern("HH");

    public String evaluate(Long timeInMS) {
        if(timeInMS == null)
            return null;
        DateTime dt = new DateTime(timeInMS);
        String dtStr = dtFormatter.print(dt);
        return dtStr;
    }
}
