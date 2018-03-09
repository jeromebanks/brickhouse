package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

/**
 *
 * Get a YPath named_struct, given an XUnit , and
 *   a YPath dimension
 *
 */
@Description(
        name = "get_ypath_struct",
        value = "returns a named_struct representation of a YPath string, given an XUnit and a YPath dimension "
)
public class GetYPathStructUDF extends GenericUDF {
    ObjectInspector xunitInspector = null;
    StringObjectInspector ypDimInspector = null;

    String usage = "get_ypath_struct takes two arguments, an XUnit and a YPath dimensions";

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if( objectInspectors.length != 2) {
            throw new UDFArgumentException(usage);
        }
        xunitInspector = XUnitUtils.ValidateXUnitObjectInspector( objectInspectors[0], usage);
        if( !(objectInspectors[1] instanceof StringObjectInspector) ) {
            throw new UDFArgumentException(usage);
        }
        ypDimInspector  = (StringObjectInspector)objectInspectors[1];

        return XUnitUtils.GetYPathInspector();
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        XUnitDesc xunit = XUnitUtils.InspectXUnit( deferredObjects[0].get(), xunitInspector);
        String ypDim = ypDimInspector.getPrimitiveJavaObject( deferredObjects[1].get()) ;

        for( YPathDesc yp : xunit.getYPaths()) {
            if( yp.getDimName().equals( ypDim)) {
                return XUnitUtils.StructObjectForYPath( yp);
            }
        }

        return null;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return XUnitUtils.DisplayString("get_ypath_struct", strings);
    }
}
