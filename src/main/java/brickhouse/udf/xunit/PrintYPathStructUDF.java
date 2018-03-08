package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * Return the YPath String which matches the value
 *   of a named_struct representation of a YPath
 *
 */
public class PrintYPathStructUDF extends GenericUDF {
    StructObjectInspector ypathStructInspector;

    String usage = ("print_ypath_struct takes an YPath named_struct as an argument.");

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if( objectInspectors.length != 1) {
            throw new UDFArgumentException("print_ypath_struct takes a YPath named_struct as an argument.");
        }

        if(!(objectInspectors[0] instanceof StructObjectInspector)) {
            throw new UDFArgumentException("print_ypath_struct takes an YPath named_struct as an argument.");
        }
        ypathStructInspector = (StructObjectInspector)XUnitUtils.ValidateYPathObjectInspector( objectInspectors[0], usage);

        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
       YPathDesc yp = XUnitUtils.InspectYPath( deferredObjects[0].get(), ypathStructInspector );
       return yp.toString();
    }

    @Override
    public String getDisplayString(String[] strings) {
       return  XUnitUtils.DisplayString("xunit_struct", strings);
    }
}
