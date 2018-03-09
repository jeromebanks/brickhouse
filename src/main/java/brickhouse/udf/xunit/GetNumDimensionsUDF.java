package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 *  Return the number of Dimensions in an XUnit
 */
@Description(
        name = "get_num_yp_dims",
        value = "return the number of YPath dimensions in an XUnit"
)
public class GetNumDimensionsUDF extends GenericUDF {
    ObjectInspector xunitInspector = null;

    String usage = "get_num_yp_dims takes an XUnit as an argument";

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if( objectInspectors.length != 1) {
            throw new UDFArgumentException(usage);
        }
        xunitInspector = XUnitUtils.ValidateXUnitObjectInspector( objectInspectors[0], usage);

        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        XUnitDesc xunit = XUnitUtils.InspectXUnit(deferredObjects[0].get(), xunitInspector);
        if( xunit == null) {
            return null;
        } else {
            return xunit.numDims();
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return XUnitUtils.DisplayString("get_num_yp_dims", strings);
    }

}
