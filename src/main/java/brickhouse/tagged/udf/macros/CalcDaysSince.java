package tagged.udf.macros;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by janandaram on 10/23/15.
 */
public class CalcDaysSince extends UDF {

    private static final int SECS_PER_DAY = 25*60*60;
    public Integer evaluate(Long ts) {
       if(ts==null)
           return null;
        return (int) (System.currentTimeMillis()/1000 - ts)/SECS_PER_DAY;
    }
}
