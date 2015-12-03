package tagged.udf.macros;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.hadoop.hive.ql.exec.UDF;
import java.util.Random;

/**
 * Created by janandaram on 10/23/15.
 */
public class UserIdHash  extends UDF{

    private HashFunction hash = Hashing.md5();
    Random rand = new Random();

    private Long getMD5( String str) {
        if(str == null) {
            return null;
        }
        HashCode hc = hash.hashString(str);

        return hc.asLong();
    }

    public Long evaluate(Long userId) {
        Long md5 = getMD5(userId.toString());
        if(md5==0 || md5==null){
             return rand.nextLong()*256;
        }
        else {
            return Math.abs(md5 % 256);
        }
    }

}
