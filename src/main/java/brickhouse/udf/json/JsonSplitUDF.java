package brickhouse.udf.json;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *   UDF to split a JSON array into individual json strings ...
 * @author jeromebanks
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
