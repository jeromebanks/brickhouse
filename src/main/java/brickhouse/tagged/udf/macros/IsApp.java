package tagged.udf.macros;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by janandaram on 10/23/15.
 */
public class IsApp extends UDF {

    public Boolean  evaluate(String userAgent) {
        if(userAgent == null)
            return null;
        if(userAgent.contains("Tagged") || userAgent.contains("Hi5"))
            return true;
        else
            return false;
    }
}
