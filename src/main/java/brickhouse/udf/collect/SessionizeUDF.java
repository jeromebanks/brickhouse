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
 *
 *
 * select
 * 	sessionize(user_id, timestamp), user_id, time_stamp, event_info
 * 
 **/


// YY: Does this belong in the date package? Maybe it'll fit better elsewhere.... hm....


// for now, just implement sessionize(uid, ts)- but we probably need sessionize(uid, ts, tolerance) later on....

import java.util.UUID;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(
		name="sessionize", 
		value="_FUNC_(int, timestamp) - Returns a session id for the given id(int) and ts(long).",
		extended="SELECT _FUNC_(uid, ts), uid, ts, event_type from foo;")

public class SessionizeUDF extends UDF {
		private long last_uid = 0;
		private long last_ts = 0;
		private String last_uuid = null;

	  
	  public String evaluate(long uid, long ts, int tolerance) {		  
		  if (uid == last_uid && TimeStampCompare(last_ts, ts, tolerance)) { 
			  last_ts = ts;
			  
			  return last_uuid;
			  
		  } else if (uid == last_uid) {
			  last_ts = ts;
			  last_uuid=UUID.randomUUID().toString();
			  return last_uuid;
			  
		  } else {
			  last_uid = uid;
			  last_ts = ts;
			  last_uuid=UUID.randomUUID().toString(); // is there a way to grab the last session of the table?
			  return last_uuid;
		  }
	  }
	  public String evaluate(long uid,  long ts) { 
		 return evaluate(uid, ts, 1800000); // 30 minute = 1800000 seconds
	  }
	  
	  private Boolean TimeStampCompare(long last_ts, long ts, int ms) { 
		  try {
			  //long difference = ts.subtract(last_ts); 
			  //return (Math.abs(difference.intValueExact()) < ms) ? true : false; 
			  long difference = ts - last_ts;
			  return (Math.abs((int)difference) < ms) ? true : false;
		  } catch (ArithmeticException e) {
			  return false;
		  }
	  }
	}