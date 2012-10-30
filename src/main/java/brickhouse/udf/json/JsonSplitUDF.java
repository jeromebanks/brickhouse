package brickhouse.udf.json;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *   UDF to split a JSON array into individual json strings ...
 *
 */

@Description(name="json_split",
             value = "_FUNC_(json) - Returns a array of JSON strings from a JSON Array"
)
public class JsonSplitUDF extends GenericUDF {
  private StringObjectInspector stringInspector;


  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    List<String> retJsonArr = new ArrayList<String>();
    try {
      String jsonString = this.stringInspector.getPrimitiveJavaObject(arguments[0].get());

      ObjectMapper om = new ObjectMapper();
      Object root = om.readValue(jsonString, Object.class);
      List<Object> rootAsMap = om.readValue(jsonString, List.class);
      for( Object jsonObj : rootAsMap) {
        if (jsonObj != null){
          String jsonStr = om.writeValueAsString(jsonObj);
          retJsonArr.add(jsonStr);
        }
      }

      return retJsonArr;


    } catch( JsonProcessingException jsonProc) {
      return null;
    } catch (IOException e) {
      return null;
    } catch (NullPointerException npe){
      return retJsonArr;
    }

  }

  @Override
  public String getDisplayString(String[] arg0) {
    return "json_split(" + arg0[0] + ")";
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    if(arguments.length != 1) {
      throw new UDFArgumentException("Usage : json_split jsonstring) ");
    }
    if(!arguments[0].getCategory().equals( Category.PRIMITIVE)) {
      throw new UDFArgumentException("Usage : json_split( jsonstring) ");
    }
    stringInspector = (StringObjectInspector) arguments[0];

    ObjectInspector valInspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

    ObjectInspector setInspector = ObjectInspectorFactory.getStandardListObjectInspector(valInspector);
    return setInspector;
  }

}
