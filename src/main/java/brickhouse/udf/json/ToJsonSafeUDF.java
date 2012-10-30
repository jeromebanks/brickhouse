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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 *  Generate a JSON string from a Map
 *
 *
 */
@Description(name="to_json_safe",
    value = "_FUNC_(a,b) - Returns a JSON string from a Map of values, also if string value starts with '{' and ends " +
        "  with '}' asumption is that value is json object and although string it will not be quoted."
)
public class ToJsonSafeUDF extends GenericUDF {
  private static final Logger LOG = Logger.getLogger(ToJsonSafeUDF.class);

  // XXX For now, just assume a simple map of values ...
  // XXX TODO extend to support standard JSON types, nested maps and array
  // XXX Use standard JSON package
  MapObjectInspector mapOI;
  BooleanObjectInspector boolOI;
  ObjectInspector mapValueOI;

  public String evaluate(Map jsonMap, boolean quoteStrings) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    Set keySet = jsonMap.keySet();
    Iterator keyIter = keySet.iterator();
    for(int i=0; i<keySet.size() -1; ++i) {
      Object key = keyIter.next();
      sb.append("\"");
      sb.append( key.toString());
      sb.append("\":");
      Object obj = jsonMap.get( key);
      if(quoteStrings && mapValueOI instanceof StringObjectInspector) {
        String primitive = ((StringObjectInspector)mapValueOI).getPrimitiveJavaObject(obj);
        System.out.println("primitive = " + primitive);
        // Simple json detection, oversimplified but should do the work.
        if (primitive.startsWith("{") && primitive.endsWith("}")) {
          sb.append(primitive);
        } else {
          sb.append("\"");
          sb.append(primitive);
          sb.append("\"");
        }
      } else if (obj instanceof Text) {
        sb.append("\"");
        sb.append(obj.toString());
        sb.append("\"");
      } else {
        if( obj != null)
          sb.append(obj.toString());
        else
          sb.append("null");
      }
      sb.append(",");
    }
    /// Do the last one
    if(keyIter.hasNext()) {
      Object key = keyIter.next();
      sb.append("\"");
      sb.append( key.toString());
      sb.append("\":");
      Object obj = jsonMap.get( key);
      if(quoteStrings && mapValueOI instanceof StringObjectInspector) {
        sb.append("\"");
        sb.append(((StringObjectInspector)mapValueOI).getPrimitiveJavaObject(obj));
        sb.append("\"");
      } else {
        sb.append(obj.toString());
      }
    }
    sb.append("}");

    String jsonStr = sb.toString();
    return jsonStr;

  }

  @Override
  public Object evaluate(DeferredObject[] args) throws HiveException {
    Map objMap = mapOI.getMap(args[0].get());
    Boolean quoteStrings = true;
    if (args.length == 2) {
     quoteStrings = boolOI.get(args[1].get());
    }
    return evaluate(objMap, quoteStrings);
  }

  @Override
  public String getDisplayString(String[] args) {
    return "to_json(" + args[0] + ")";
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] args)
      throws UDFArgumentException {
    if(args.length != 1 && args.length != 2) {
      throw new UDFArgumentException(" ToJson currently only takes a map as an argument");
    }
    ObjectInspector oi = args[0];
    if(oi.getCategory() != Category.MAP) {
      throw new UDFArgumentException(" ToJson currently only takes a map as an argument");
    }
    mapOI = (MapObjectInspector)oi;
    mapValueOI =  mapOI.getMapValueObjectInspector();
    if (mapValueOI.getCategory() != Category.PRIMITIVE) {
      throw new UDFArgumentException(" At the moment we do not handle complex types. Input map key is of category " +
          mapValueOI.getCategory().toString());
    }

    if (args.length == 2) {
      ObjectInspector oiSecond = args[1];
      boolOI = (BooleanObjectInspector)oiSecond;
    }

    LOG.debug(" To JSON input type is " + mapOI +
        " key = " + mapOI.getMapKeyObjectInspector().getTypeName() +
        " value = " + mapOI.getMapValueObjectInspector().getTypeName());

    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }

}
