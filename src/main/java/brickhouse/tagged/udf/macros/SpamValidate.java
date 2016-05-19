package tagged.udf.macros;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

/**
 * Created by janandaram on 11/13/15.
 */
public class SpamValidate extends UDF {

    private Long getFormattedTime(String strDate, DateTimeFormatter dtFormatter) {
        DateTime dt = dtFormatter.parseDateTime(strDate);
        return dt.toDate().getTime();
    }

    public String evaluate(String interestedDate, Long dateValidated, Boolean spammer) {
        DateTimeFormatter dtFormatter = null;
        if(interestedDate != null){
            if(interestedDate.length()==8)
                dtFormatter = org.joda.time.format.DateTimeFormat.forPattern("YYYYMMdd");
            else
                dtFormatter = org.joda.time.format.DateTimeFormat.forPattern("YYYYMMddHH");
            Long interestedDateInMS = getFormattedTime(interestedDate, dtFormatter);
            if(spammer) {
                if(dateValidated != null && interestedDateInMS >= dateValidated)
                    return "spammer-validated";
                else
                    return "spammer-nonvalidated";
            }else {
                if(dateValidated != null && interestedDateInMS >= dateValidated)
                    return "nonspammer-validated";
                else
                    return "nonspammer-nonvalidated";
            }
        }
        return null;

    }
}
