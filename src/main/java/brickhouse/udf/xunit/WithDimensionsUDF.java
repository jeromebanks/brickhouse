package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.HashSet;
import java.util.List;

/**
 *
 * Return true only if the given XUnit has
 *   a specified set of dimensions,
 *   ( and only those dimensions )
 *
 */
@Description(
        name="contains_only_yp_dims",
        value="return true if and only if the xunit contains only the specified dimensions")
public class WithDimensionsUDF extends GenericUDF {
    ObjectInspector xunitInspector = null;
    ListObjectInspector dimsInspector = null;
    StringObjectInspector dimInspector = null;

    String usage = "contains_only_yp_dims takes an XUnit and a array of YPath dimensions ( and only those dimensions).";

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if(objectInspectors.length != 2) {
            throw new UDFArgumentException(usage);
        }
        xunitInspector = XUnitUtils.ValidateXUnitObjectInspector(objectInspectors[0], usage);
        if( objectInspectors[1].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentException(usage);
        }
        dimsInspector = (ListObjectInspector)objectInspectors[1];
        if( !(dimsInspector.getListElementObjectInspector() instanceof StringObjectInspector) ) {
            throw new UDFArgumentException(usage);
        }
        dimInspector = (StringObjectInspector)dimsInspector.getListElementObjectInspector();


        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;

    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        XUnitDesc xunit = XUnitUtils.InspectXUnit( deferredObjects[0].get(), xunitInspector);
        if(xunit.isGlobal()) {
            return false;
        }

        List<String> dims = XUnitUtils.InspectStringList( deferredObjects[1].get(), dimsInspector);

        if( xunit.numDims() != dims.size() ) {
            return false;
        }

        for( YPathDesc yp : xunit.getYPaths()) {
            if( ! dims.contains( yp.getDimName())) {
                return false;
            }
        }

        return true;
    }


    @Override
    public String getDisplayString(String[] strings) {
        return XUnitUtils.DisplayString("contains_only_yp_dims", strings);
    }
}
