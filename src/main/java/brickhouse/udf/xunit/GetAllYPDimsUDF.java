package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 *  Return all the YPath dimensions from an XUnit
 *
 *  Takes in either an XUnit string or a named_struct,
 *   and returns an array of strings containing
 *     the YPath dimensions
 *
 */
@Description(
        name = "get_all_yp_dims",
        value = "return all the dimensions in an XUnit as an array<string>"
)
public class GetAllYPDimsUDF extends GenericUDF {
    ObjectInspector xunitInspector  = null;

    String usage = "get_all_yp_dims takes either an XUnit string or named_struct as an argument, and returns an array of strings";

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if( objectInspectors.length != 1) {
            throw new UDFArgumentException(usage);
        }
        xunitInspector = XUnitUtils.ValidateXUnitObjectInspector( objectInspectors[0], usage);


        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        XUnitDesc xunit = XUnitUtils.InspectXUnit( deferredObjects[0].get(), xunitInspector);
        if( xunit == null) {
            return null;
        }
        List<String> ypDims = new ArrayList<String>();
        for( YPathDesc yp : xunit.getYPaths()) {
           ypDims.add( yp.getDimName());
        }
        return ypDims; /// Should interpret List as well as object array
    }

    @Override
    public String getDisplayString(String[] strings) {
        return XUnitUtils.DisplayString( "get_all_yp_dims", strings);
    }

}
