package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.List;

/**
 *  Construct a YPath named_struct, given
 *   a dimension name, and a list of attribute names
 *    and a list of attribute values
 *
 */

@Description(
        name="construct_ypath",
        value="return a YPath named_struct given a dimension name, and a list of attribute names and values.")
public class ConstructYPathUDF extends GenericUDF {
    StringObjectInspector dimInspector = null;
    ListObjectInspector attrNamesInspector = null;
    ListObjectInspector attrValuesInspector = null;

    String usage = "construct_ypath takes a YPath dimension , an array of attribute names, and an array of attribute values.";

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if(objectInspectors.length != 3) {
            throw new UDFArgumentException(usage);
        }
        if( ! ( objectInspectors[0] instanceof  StringObjectInspector) ) {
            throw new UDFArgumentException(usage);
        }
        dimInspector = (StringObjectInspector)objectInspectors[0];
        if( ! ( objectInspectors[1] instanceof  ListObjectInspector)
                || ! ( objectInspectors[2] instanceof ListObjectInspector)) {
            throw new UDFArgumentException(usage);
        }
        attrNamesInspector = (ListObjectInspector)objectInspectors[1];
        attrValuesInspector = (ListObjectInspector)objectInspectors[2];
        if( ! ( attrNamesInspector.getListElementObjectInspector() instanceof  StringObjectInspector)
                || ! ( attrValuesInspector.getListElementObjectInspector() instanceof StringObjectInspector)) {
            throw new UDFArgumentException(usage);
        }

        return XUnitUtils.GetYPathInspector();

    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        String dimName = dimInspector.getPrimitiveJavaObject( deferredObjects[0].get());
        List<String> attrNames = XUnitUtils.InspectStringList( deferredObjects[1].get(), attrNamesInspector);
        List<String> attrValues = XUnitUtils.InspectStringList( deferredObjects[1].get(), attrValuesInspector);

        YPathDesc ypath = new YPathDesc(dimName, attrNames.toArray( new String[attrNames.size()]), attrValues.toArray( new String[attrNames.size()]));

        return XUnitUtils.StructObjectForYPath( ypath);
    }


    @Override
    public String getDisplayString(String[] strings) {
        return XUnitUtils.DisplayString("construct_ypath", strings);
    }
}
