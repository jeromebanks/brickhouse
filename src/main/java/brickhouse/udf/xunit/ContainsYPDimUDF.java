package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

/**
 *
 * Return true only if the given XUnit
 * contains a specified YPath dimension
 *
 */
@Description(
        name="contains_yp_dim",
        value="return true if and only if the xunit contains the specified YPath dimension. ")
public class ContainsYPDimUDF extends GenericUDF {
    ObjectInspector xunitInspector = null;
    StringObjectInspector ypDimInspector = null;

    String usage = "contains_yp_dim takes an XUnit and a YPath dimension as arguments.";

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if(objectInspectors.length != 2) {
            throw new UDFArgumentException(usage);
        }
        xunitInspector = XUnitUtils.ValidateXUnitObjectInspector(objectInspectors[0], usage);
        if( !( objectInspectors[1] instanceof StringObjectInspector ) ) {
            throw new UDFArgumentException(usage);
        }
        ypDimInspector = (StringObjectInspector)objectInspectors[1];

        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;

    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        XUnitDesc xunit = XUnitUtils.InspectXUnit( deferredObjects[0].get(), xunitInspector);
        if(xunit.isGlobal()) {
            return false;
        }


        String ypDim = ypDimInspector.getPrimitiveJavaObject( deferredObjects[1].get() );
        return xunit.containsDim( ypDim);
    }


    @Override
    public String getDisplayString(String[] strings) {
        return XUnitUtils.DisplayString("contains_yp_dim", strings);
    }
}
