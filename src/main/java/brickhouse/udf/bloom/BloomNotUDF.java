package brickhouse.udf.bloom;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.util.bloom.Filter;


@Description(
		 name = "bloom_not",
		 value =  " Returns the logical NOT of a bloom filters; representing the set of values NOT in bloom1   \n " +
		          "_FUNC_(string bloom) "
		)
public class BloomNotUDF extends UDF {

	public String evaluate( String bloomStr ) throws IOException {
		Filter bloom = BloomFactory.GetBloomFilter( bloomStr);
		
		/// Perform a logical not 
		bloom.not();
		
		return BloomFactory.WriteBloomToString( bloom);
	}
}
