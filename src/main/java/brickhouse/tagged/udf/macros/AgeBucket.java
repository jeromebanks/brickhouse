package tagged.udf.macros;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by janandaram on 10/23/15.
 */
public class AgeBucket extends UDF {

    public String evaluate(Integer age) {
        if(age==null) return null;
        else if(age < 18 ) return "<18";
        else if(age <= 24) return "18-24";
        else if(age <= 34) return "25-34";
        else if(age <= 44) return "35-44";
        else if(age <= 54) return "45-54";
        else return "55+";
    }

}
