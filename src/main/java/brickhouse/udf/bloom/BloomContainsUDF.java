package brickhouse.udf.bloom;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Filter;
import org.apache.hadoop.util.bloom.Key;

/**
 *   Returns true if the bloom (probably) contains the string
 *   
 * @author jeromebanks
 *
 */
@Description(
		 name = "bloom_contains",
		 value =  " Returns true if the referenced bloom filter contains the key.. \n " +
		          "_FUNC_(string key, string bloomfilter) "
		)
public class BloomContainsUDF extends UDF {


	public Boolean evaluate( String key, String bloomFilter) throws HiveException {
		Filter bloom = BloomFactory.GetBloomFilter(bloomFilter);
		if( bloom != null) {
			return bloom.membershipTest( new Key(key.getBytes()));
		} else {
			throw new HiveException("Unable to find bloom " + bloomFilter);
		}
	}

}
