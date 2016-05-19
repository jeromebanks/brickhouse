package tagged.udf.macros;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by janandaram on 10/23/15.
 */
public class FindMin extends UDF {

    public Long evaluate(Long x, Long y) {
        if(x==null && y != null) return y;
        else if(x !=null && y == null) return x;
        else return (x < y ? x : y);
    }
}
