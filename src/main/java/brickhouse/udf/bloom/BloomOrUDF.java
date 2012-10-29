package brickhouse.udf.bloom;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.util.bloom.Filter;



@Description(
		 name = "bloom_or",
		 value =  " Returns the logical OR of two bloom filters; representing the intersection of values in either bloom1 OR bloom2  \n " +
		          "_FUNC_(string bloom1, string bloom2) "
		)
public class BloomOrUDF extends UDF {

	public String evaluate( String bloom1Str, String bloom2Str ) throws IOException {
		Filter bloom1 = BloomFactory.GetBloomFilter( bloom1Str);
		Filter bloom2 = BloomFactory.GetBloomFilter( bloom2Str);
		
		bloom1.or( bloom2);
		
		return BloomFactory.WriteBloomToString( bloom1);
	}
}
