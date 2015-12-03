package tagged.udf.macros;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

/**
 * Created by janandaram on 10/23/15.
 */
public class SpamDetector extends UDF {



    private Long getMin(Long x, Long y) {
        if(x==null && y==null) return null;
        else if(x==null && y != null) return y;
        else if(x !=null && y == null) return x;
        else return (x < y ? x : y);
    }

    private Long getFormattedTime(String strDate, DateTimeFormatter dtFormatter) {
        DateTime dt = dtFormatter.parseDateTime(strDate);
        return dt.toDate().getTime();
    }

    public Boolean evaluate(String interested_date , Long date_cancelled ,
                            Long date_boxed , Long date_spammer_added , Long date_spammer_removed) {

        Long minDate = getMin(getMin(date_cancelled,date_boxed),date_spammer_added);
        DateTimeFormatter dtFormatter = null;
        Long minDate2 =  getMin(date_cancelled,date_boxed);
        if(minDate2==null)
            minDate2=0L;

        if(interested_date.length()==8)
            dtFormatter = org.joda.time.format.DateTimeFormat.forPattern("YYYYMMdd");
        else
            dtFormatter = org.joda.time.format.DateTimeFormat.forPattern("YYYYMMddHH");

        Long interestedDateInMS = getFormattedTime(interested_date, dtFormatter);
        //Long dateSpammerRemovedInMS = getFormattedTime(date_spammer_removed,dtFormatter);

        int retVal=0;
        if( (date_cancelled==null && date_boxed == null && date_spammer_added == null) ||
            (date_spammer_removed == null && interestedDateInMS < minDate) ||
            (date_spammer_removed != null &&
                    (
                        (date_spammer_added > date_spammer_removed &&
                                interestedDateInMS < minDate && interestedDateInMS > date_spammer_removed)
                        || (date_spammer_added < date_spammer_removed &&
                                (interestedDateInMS < minDate ||
                                 (interestedDateInMS > date_spammer_removed &&
                                         (interestedDateInMS < minDate2 || (date_cancelled == null && date_boxed == null)
                                         )
                                 )
                                )
                           )
                    )
                ))
            retVal = 1;
        return retVal==1 ? false : true;

    }

}
