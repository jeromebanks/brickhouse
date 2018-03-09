package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.List;

/**
 *
 * Return true only if the given XUnit
 * contains a specified YPath value
 *
 */
@Description(
        name="contains_ypath",
        value="return true if and only if the xunit contains the specified YPath. ")
public class ContainsYPathUDF extends GenericUDF {
    ObjectInspector xunitInspector = null;
    ObjectInspector ypathInspector = null;

    String usage = "contains_ypath takes an XUnit and a YPath as arguments.";

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if(objectInspectors.length != 2) {
            throw new UDFArgumentException(usage);
        }
        xunitInspector = XUnitUtils.ValidateXUnitObjectInspector(objectInspectors[0], usage);
        if( objectInspectors[1].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentException(usage);
        }
        ypathInspector = XUnitUtils.ValidateYPathObjectInspector(objectInspectors[1], usage);


        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;

    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        XUnitDesc xunit = XUnitUtils.InspectXUnit( deferredObjects[0].get(), xunitInspector);
        if(xunit.isGlobal()) {
            return false;
        }

        YPathDesc ypath = XUnitUtils.InspectYPath( deferredObjects[1].get(), ypathInspector);
        YPathDesc checkYPath = xunit.getYPath( ypath.getDimName());
        if( checkYPath == null) {
            return false;
        }

        if( ypath.getAttributeNames().length != checkYPath.getAttributeNames().length) {
            return false;
        }
        for(int i=0; i< ypath.getAttributeNames().length -1; ++i) {
            String attrName = ypath.getAttributeNames()[i];
            String checkAttrName = checkYPath.getAttributeNames()[i];

            if( !checkAttrName.equals( attrName)) {
                return false;
            }

            String attrValue = ypath.getAttributeValues()[i];
            String checkAttrValue = checkYPath.getAttributeValues()[i];

            if( !checkAttrValue.equals( attrValue)) {
                return false;
            }
        }

        return true;
    }


    @Override
    public String getDisplayString(String[] strings) {
        return XUnitUtils.DisplayString("contains_ypath", strings);
    }
}
