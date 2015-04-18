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

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.DecimalFormat;

/**
 * Create a salted key from a BigInt, which
 * may not be distributed evenly across the
 * most significant bits ( ie. some large values, some low values)
 * but are distributed evenly across the low bits
 * ( ie. modulo 1000 )
 */
@Deprecated
public class SaltedBigIntUDF extends UDF {
    private DecimalFormat saltFormat = new DecimalFormat("0000");

    public String evaluate(Long id) {
        StringBuilder sb = new StringBuilder();
        sb.append(saltFormat.format(id % 10000));
        sb.append(":");
        sb.append(id.toString());

        return sb.toString();
    }
}
