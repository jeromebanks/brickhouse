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
 *  Generate an HBase salt for a string, which can be used for building an hbase evenly balanced key across N regions.
 *
 */
@Description(name="hbase_salt",
value = "_FUNC_(keyStr,numRegions) - Returns an HBase salt evenly across regions"
)
public class GenerateSaltUDF extends UDF {

	public String evaluate(String keyStr, int numRegions ) {
		int sumChars = 0;
		for(int i=0; i<keyStr.length(); ++i) {
			sumChars += (int)keyStr.charAt(i);
		}
		
		retrun String.format("%02X", sumChars % numRegions));
	}
}
