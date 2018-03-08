package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.ArrayList;
import java.util.List;

/**
 *  Parse an XUnit String, and return a NamedStruct
 *    representing the YPath
 *
 */
public class PrintXUnitStructUDF extends GenericUDF {
    StructObjectInspector xunitStructInspector;

    String usage = ("print_xunit_struct takes an XUnit name_struct as an argument.");

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if( objectInspectors.length != 1) {
            throw new UDFArgumentException("print_xunit_struct takes an XUnit named_struct as an argument.");
        }

        if(!(objectInspectors[0] instanceof StructObjectInspector)) {
           throw new UDFArgumentException("print_xunit_struct takes an XUnit name_struct as an argument.");
        }
        xunitStructInspector = (StructObjectInspector)XUnitUtils.ValidateXUnitObjectInspector( objectInspectors[0], usage);


        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        XUnitDesc xunit = XUnitUtils.InspectXUnit( deferredObjects[0].get(), xunitStructInspector);
        return xunit.toString();
    }

    @Override
    public String getDisplayString(String[] strings) {
        return XUnitUtils.DisplayString("print_xunit_struct", strings);
    }
}
