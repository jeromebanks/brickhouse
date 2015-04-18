package brickhouse.hbase;
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

/**
 * Generate an HBase Key for a string, which
 * should be evenly balanced across N regions.
 */
@Description(name = "hbase_balanced_key",
        value = "_FUNC_(keyStr,numRegions) - Returns an HBase key balanced evenly across regions"
)
public class GenerateBalancedKeyUDF extends UDF {

    public String evaluate(String keyStr, int numRegions) {
        int sumChars = 0;
        for (int i = 0; i < keyStr.length(); ++i) {
            sumChars += (int) keyStr.charAt(i);
        }

        StringBuilder sb = new StringBuilder();
        sb.append(Integer.toHexString(sumChars % numRegions).toUpperCase());
        sb.append(':');
        sb.append(keyStr);

        return sb.toString();
    }
}
