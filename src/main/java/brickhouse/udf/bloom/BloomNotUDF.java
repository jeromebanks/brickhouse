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
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.util.bloom.Filter;

import java.io.IOException;


@Description(
        name = "bloom_not",
        value = " Returns the logical NOT of a bloom filters; representing the set of values NOT in bloom1   \n " +
                "_FUNC_(string bloom) "
)
public class BloomNotUDF extends UDF {

    public String evaluate(String bloomStr) throws IOException {
        Filter bloom = BloomFactory.GetBloomFilter(bloomStr);

        /// Perform a logical not
        bloom.not();

        return BloomFactory.WriteBloomToString(bloom);
    }
}
