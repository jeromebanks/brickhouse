package tagged.udf.macros;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by janandaram on 10/23/15.
 */
public class ABS extends UDF {
    public Long evaluate(Long val)
    {
        if(val==null)
            return null;
        return val >= 0 ? val : (-1*val);
    }
}
