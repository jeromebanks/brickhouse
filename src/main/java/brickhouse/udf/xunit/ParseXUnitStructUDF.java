package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.ArrayList;

/**
 *  Parse an XUnit String, and return a NamedStruct
 *    representing the YPath
 *
 */
@Description(
        name = "parse_xunit_string",
        value = "return the number of YPath dimensions in an XUnit"
)
public class ParseXUnitStructUDF extends GenericUDF {
    ObjectInspector xunitInspector;

    String usage = ("parse_xunit_string takes an XUnit String as an argument.");

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if( objectInspectors.length != 1) {
            throw new UDFArgumentException(usage);
        }
        xunitInspector = XUnitUtils.ValidateXUnitObjectInspector( objectInspectors[0], usage);

        if(!(objectInspectors[0] instanceof StringObjectInspector )) {
            throw new UDFArgumentException(usage);
        }


        return XUnitUtils.GetXUnitInspector();

    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        XUnitDesc xunit = XUnitUtils.InspectXUnit( deferredObjects[0].get(), xunitInspector);
        System.out.println(" XUNIT = " + xunit.toString() );

        return XUnitUtils.StructObjectForXUnit( xunit);
    }

    @Override
    public String getDisplayString(String[] strings) {
        return XUnitUtils.DisplayString("parse_xunit_string", strings);
    }

}
