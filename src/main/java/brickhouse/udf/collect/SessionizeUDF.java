package brickhouse.udf.collect;
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
 *
 */

import java.util.UUID;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 
 * Creates a session id for an index and a time stamp. Default session length is 30 minute = 1800000 milliseconds
 *   
 *   The input to this function is assumed to be sorted by a user_key , and by time, and is useful for creating sessions from
 *     user activity data. (i.e. web logs ).  The timestamp is assumed to be bigint, representing the number of milliseconds from
 *     the beginning of the epoch.
 *     
 *    The input needs to be sorted and partitioned using Hive 'SORT' and 'DISTRIBUTE' clauses.  Example usage would be :
 *      
 *    SELECT user_key, 
 *          session_id,
 *          min( tstamp) as session_start,
 *          max( tstamp ) as session_end
 *    FROM 
 *      ( SELECT user_key,
 *               sessionize( user_key, tstamp )
 *          FROM weblogs
 *       DISTRIBUTE BY user_key
 *       SORT BY user_key, tsamp 
 *     ) sz
 *   GROUP BY
 *     user_key, session_id;
 * 
 */
@Description(
		name="sessionize", 
		value="_FUNC_(string, timestamp) - Returns a session id for the given id and ts(long). Optional third parameter to specify interval tolerance in milliseconds",
		extended="SELECT _FUNC_(uid, ts), uid, ts, event_type from foo;")

public class SessionizeUDF extends UDF {
		private String lastUid = null;
		private long lastTS = 0;
		private String lastUUID = null;

	  
	  public String evaluate(String uid, long ts, int tolerance) {
		  if (uid.equals(lastUid) && timeStampCompare(lastTS, ts, tolerance)) { 
			  lastTS = ts;			  
		  } else if (uid.equals(lastUid)) {
			  lastTS = ts;
			  lastUUID=UUID.randomUUID().toString();
		  } else {
			  lastUid = uid;
			  lastTS = ts;
			  lastUUID=UUID.randomUUID().toString();
		  }
		  return lastUUID;
	  }

	  public String evaluate(String uid,  long ts) { 
		 return evaluate(uid, ts, 1800000);
	  }
	  
	  private Boolean timeStampCompare(long lastTS, long ts, int ms) { 
		  try {
			  long difference = ts - lastTS;
			  return (Math.abs((int)difference) < ms) ? true : false;
		  } catch (ArithmeticException e) {
			  return false;
		  }
	  }
	}