package brickhouse.udf.sketch;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 *  Calculate the MD5 hash for a String.
 *   
 *    Useful for sketching ...
 * @author jeromebanks
 *
 */
@Description(name="hash_md5",
value = "_FUNC_(x) - Hash MD5. "
)
public class HashMD5UDF extends UDF {
	private HashFunction hash = Hashing.md5();


	public Long evaluate( String str) {
		HashCode hc = hash.hashString( str);

		return hc.asLong();
	}
}
