package tagged.udf.macros;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by janandaram on 10/23/15.
 */
public class CalcAge extends UDF {
    public Integer  evaluate(Long bd) {
        if(bd==null)
            return null;
        return (int)((System.currentTimeMillis() / 1000) - bd) / 31557600;
    }
}
