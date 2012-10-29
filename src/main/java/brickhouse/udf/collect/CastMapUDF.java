package brickhouse.udf.collect;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.TreeMap;

/**
 * Cast an Map to the string to string map
 * @author nemanja@klout (Nemanja Spasojevic)
 *
 * Based on CastArrayUDF.
 *
 */
public class CastMapUDF extends GenericUDF {
  private static final Logger LOG = Logger.getLogger(CastMapUDF.class);
  private MapObjectInspector mapInspector;


  public Map<String, String> evaluate(Map<Object, Object> strMap) {
    Map<String, String> newMap = new TreeMap<String, String>();
    for(Object keyObj : strMap.keySet() ) {
      newMap.put(keyObj.toString(), strMap.get(keyObj).toString());
    }
    return newMap;
  }

  @Override
  public Map<String, String> evaluate(DeferredObject[] arg0) throws HiveException {
    Map argMap = mapInspector.getMap(arg0[0].get());
    if(argMap != null)
      return evaluate(argMap);
    else
      return null;
  }

  @Override
  public String getDisplayString(String[] arg0) {
    return "cast_map()";
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arg0)
      throws UDFArgumentException {
    this.mapInspector = (MapObjectInspector) arg0[0];
    LOG.info( " Cast Map input type is " + mapInspector +
        " key = " + mapInspector.getMapKeyObjectInspector().getTypeName() +
        " value = " + mapInspector.getMapValueObjectInspector().getTypeName());
    ObjectInspector returnType = ObjectInspectorFactory.getStandardMapObjectInspector(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    return returnType;
  }
}
