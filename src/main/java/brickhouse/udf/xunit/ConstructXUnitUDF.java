package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.List;

/**
 *  Construct an XUnit named_struct,
 *   given an Array of YPaths.
 *
 *   If no YPaths are specified, the Global XUnit is returned
 */

@Description(
        name="construct_xunit",
        value="return a XUniut named_struct given an array of YPaths")
public class ConstructXUnitUDF extends GenericUDF {
    ListObjectInspector ypathListInspector = null;
    ObjectInspector ypathInspector = null;

    String usage = "construct_xunit takes an array of YPaths.";

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if(objectInspectors.length != 1) {
            throw new UDFArgumentException(usage);
        }
        if(  objectInspectors[0].getCategory() != ObjectInspector.Category.LIST ) {
            throw new UDFArgumentException(usage);
        }
        ypathListInspector = (ListObjectInspector) objectInspectors[0];
        ypathInspector = XUnitUtils.ValidateYPathObjectInspector( ypathListInspector.getListElementObjectInspector(), usage);


        return XUnitUtils.GetXUnitInspector();

    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Object ypathsObj = deferredObjects[0].get();
        int numYPaths = ypathListInspector.getListLength(ypathsObj);
        YPathDesc[] ypaths = new YPathDesc[numYPaths];
        for(int i=0; i<numYPaths; ++i) {
            Object ypObj = ypathListInspector.getListElement( ypathsObj, i);
            YPathDesc ypath = XUnitUtils.InspectYPath( ypObj, ypathInspector);
            ypaths[i] = ypath;
        }
        XUnitDesc xunit = new XUnitDesc(ypaths);

        return XUnitUtils.StructObjectForXUnit( xunit);
    }


    @Override
    public String getDisplayString(String[] strings) {
        return XUnitUtils.DisplayString("construct_xunit", strings);
    }
}
