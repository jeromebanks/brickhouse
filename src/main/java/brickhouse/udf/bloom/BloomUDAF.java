package brickhouse.udf.bloom;
/**
 * Copyright 2012 Klout, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.util.bloom.Filter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.log4j.Logger;

import java.io.IOException;


/**
 * Construct a BloomFilter by aggregating on keys
 * <p/>
 * Uses hadoop util BloomFilter class
 * Use with bloom_contains( key, bloomfile );
 * <p/>
 * insert overwrite local directory bloomfile
 * select bloom( ks_uid )
 * from big_table
 * where premise = true;
 * <p/>
 * add file bloomfile;
 * <p/>
 * select ks_uid
 * from other_big_table
 * where bloom_contains( key, distributed_bloom('bloomfile') );
 *
 * @author jeromebanks
 */
@Description(
        name = "bloom",
        value = " Constructs a BloomFilter by aggregating a set of keys \n " +
                "_FUNC_(string key) \n"
)
public class BloomUDAF extends UDAF {
    private static final Logger LOG = Logger.getLogger(BloomUDAF.class);
    //// Convert to GenericUDAF .. non-generic is broken ..


    public static class BloomUDAFEvaluator implements UDAFEvaluator {
        private Filter bloomFilter;

        /*
         */
        public void init() {
            bloomFilter = BloomFactory.NewBloomInstance();
            /**
             try {
             ///LOG.info("INIT BLOOM " + BloomFactory.WriteBloomToString(bloomFilter));
             } catch (IOException e) {
             // TODO Auto-generated catch block
             e.printStackTrace();
             }
             **/
        }


        public boolean iterate(String key) {
            if (key != null) {
                if (bloomFilter == null) {
                    init();
                }
                bloomFilter.add(new Key(key.getBytes()));

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
                if (bloomFilter != null) {
                    return BloomFactory.WriteBloomToString(bloomFilter);
                } else {
                    return null;
                }
            } catch (IOException e) {
                LOG.error(" Error while evaluating Bloom ", e);
                throw new HiveException("Error while evaluating Bloom");
            }
        }

        public boolean merge(String partial) {
            try {
                if (bloomFilter == null) {
                    bloomFilter = BloomFactory.ReadBloomFromString(partial);
                    ///LOG.info(" read bloom from partial " + BloomFactory.WriteBloomToString(bloomFilter));
                    return true;
                } else {
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
