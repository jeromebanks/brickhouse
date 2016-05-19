package tagged.udf.macros;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by janandaram on 10/23/15.
 */
public class DayGrouping extends UDF {

    public String evaluate(Double ds) {
        if(ds == null) return null;
        else if( ds <= 1.0) return ("1");
        else if( ds <= 3.0) return ("3");
        else if( ds <= 5.0) return ("5");
        else if( ds <= 7.0) return ("7");
        else if( ds <= 14.0) return ("14");
        else if( ds <= 30.0) return ("30");
        else if( ds <= 30.0) return ("30");
        else if( ds <= 60.0) return ("60");
        else if( ds <= 90.0) return  ("90");
        else if( ds <= 120.0) return ("120");
        else return (">120");
    }

}
