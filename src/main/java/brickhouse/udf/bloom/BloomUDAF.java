package brickhouse.udf.bloom;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Filter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.apache.log4j.Logger;


/**
 *   Construct a BloomFilter by aggregating on keys
 *   
 *   Uses hadoop util BloomFilter class
 *  Use with bloom_contains( key, bloomfile );
 *  
 *  insert overwrite local directory bloomfile
 *  select bloom( ks_uid )
 *   from big_table
 *    where premise = true;
 *    
 *   add file bloomfile; 
 *   
 *  select ks_uid 
 *  from other_big_table
 *  where bloom_contains( key, distributed_bloom('bloomfile') );
 *    
 *   
 * @author jeromebanks
 *
 */
@Description(
		 name = "bloom",
		 value =  " Constructs a BloomFilter by aggregating a set of keys \n " +
		          "_FUNC_(string key) \n" 
		)
public class BloomUDAF extends UDAF {
	private static final Logger LOG = Logger.getLogger( BloomUDAF.class);
	//// Convert to GenericUDAF .. non-generic is broken ..
	

	public static class BloomUDAFEvaluator implements UDAFEvaluator {
		private Filter bloomFilter;

		/*
		 */
		public void init() {
			bloomFilter =  BloomFactory.NewBloomInstance();
			/**
			try {
				///LOG.info("INIT BLOOM " + BloomFactory.WriteBloomToString(bloomFilter));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			**/
		}

		
		public boolean iterate( String key) {
			if( key != null) {
			  if( bloomFilter == null) {
				  init();
			  }
			  bloomFilter.add( new Key(key.getBytes()));
			  
			  /**
			  try {
				///LOG.info( "BloomFilter is " + BloomFactory.WriteBloomToString(bloomFilter ) + " after adding Key " +key);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			**/
			  
			}
			return true;
		}
		
		public String terminatePartial() throws HiveException {
			/**
			try {
				///LOG.info(" Terminate Partial " + BloomFactory.WriteBloomToString(bloomFilter) );
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			**/
			return terminate();
		}

		public String terminate() throws HiveException {
			try {
				if( bloomFilter != null) {
					return BloomFactory.WriteBloomToString(bloomFilter);
				} else { 
					return null;
				}
			} catch (IOException e) {
				LOG.error(" Error while evaluating Bloom ", e);
				throw new HiveException( "Error while evaluating Bloom");
			}
		}
		
		public boolean merge( String partial) {
			try {
				if( bloomFilter == null) {
					bloomFilter = BloomFactory.ReadBloomFromString(partial);
					///LOG.info(" read bloom from partial " + BloomFactory.WriteBloomToString(bloomFilter));
					return true;
				} else{
					///LOG.info(" ORng with merged before " + BloomFactory.WriteBloomToString(bloomFilter) );
					Filter other = BloomFactory.ReadBloomFromString(partial);
					///LOG.info("ORng with merged other " + BloomFactory.WriteBloomToString(other) );
					bloomFilter.or(other);
					///LOG.info(" ORing with merged after " + BloomFactory.WriteBloomToString(bloomFilter) );
					return true;
				}
			} catch (IOException e) {
				LOG.error(" Error while evaluating Bloom ", e);
				return false;
			}
		}

	}

}
