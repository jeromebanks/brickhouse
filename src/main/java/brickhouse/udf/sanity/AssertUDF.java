package brickhouse.udf.sanity;
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
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 *
 */
@Description(
        name = "assert",
        value = " Asserts in case boolean input is false. Optionally it asserts with message if input string provided. \n " +
                "_FUNC_(boolean) \n" +
                "_FUNC_(boolean, string) "
)
public class AssertUDF extends UDF {

    public String evaluate(Boolean doNotThrowAssertion, String assertionMessage) throws HiveException {
        if (doNotThrowAssertion) {
            return "OK";
        }
        throw (assertionMessage == null) ? new HiveException() : new HiveException(assertionMessage);
    }

    public String evaluate(Boolean doNotThrowAssertion) throws HiveException {
        return evaluate(doNotThrowAssertion, null);
    }
}
