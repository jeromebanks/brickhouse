package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.ArrayList;
import java.util.List;

/**
 *  Parse a YPath String, and return a NamedStruct
 *    representing the YPath
 *
 * The type of a YPath named_struct is
 *   struct<dim:string,attributes:array<struct<attribute_name:string,attribute_value:string>>>
 *
 */
@Description(
        name = "parse_ypath_string",
        value = "return the number of YPath dimensions in an XUnit"
)
public class ParseYPathStructUDF extends GenericUDF {
    StringObjectInspector ypathStrInspector = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if( objectInspectors.length != 1 || !(objectInspectors[0] instanceof StringObjectInspector)) {
           throw new UDFArgumentException("parse_ypath_string takes a String as an argument");
        }
        ypathStrInspector = (StringObjectInspector)objectInspectors[0];

        return XUnitUtils.GetYPathInspector();
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        YPathDesc ypath = XUnitUtils.InspectYPath( deferredObjects[0].get(), ypathStrInspector);
        String ypathStr = ypathStrInspector.getPrimitiveJavaObject(deferredObjects[0].get());


        return  XUnitUtils.StructObjectForYPath( ypath);
    }


    @Override
    public String getDisplayString(String[] strings) {
        return XUnitUtils.DisplayString("parse_ypath_string", strings);
    }

}
