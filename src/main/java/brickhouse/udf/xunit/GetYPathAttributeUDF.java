package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

/**
 *  Get a particular attribute from a YPath
 *    within an  XUnit
 *   Takes in either an XUnit string, or named_struct
 */
public class GetYPathAttributeUDF extends GenericUDF {
    ObjectInspector xunitInspector = null;
    StringObjectInspector ypDimInspector = null;
    StringObjectInspector ypAttrInspector = null;


    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        String usage = "get_ypath_attribute takes three arguments, an XUnit, a YPath dimension, and a YPath attribute";
        if( objectInspectors.length != 3 || objectInspectors.length != 2) {
            throw new UDFArgumentException(usage);
        }
        xunitInspector = XUnitUtils.ValidateXUnitObjectInspector( objectInspectors[0], usage);
        if( ! (objectInspectors[1] instanceof StringObjectInspector) ) {
            throw new UDFArgumentException(usage);
        }
        /// If you just pass in the ypDim, we want the attibute names
        ypDimInspector = (StringObjectInspector)objectInspectors[1];
        if( objectInspectors.length == 3) {
            if (!(objectInspectors[2] instanceof StringObjectInspector)) {
                throw new UDFArgumentException(usage);
            }
            ypAttrInspector = (StringObjectInspector) objectInspectors[2];
        }

        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        XUnitDesc xunit = XUnitUtils.InspectXUnit( deferredObjects[0].get(), xunitInspector);
        String ypDim = ypDimInspector.getPrimitiveJavaObject( deferredObjects[1].get());
        String ypAttr;
        if(ypAttrInspector != null) {
            ypAttr = ypAttrInspector.getPrimitiveJavaObject(deferredObjects[2].get());
        } else {
            ypAttr = ypDim;
        }


        if(xunit.isGlobal())  {
            return null;
        } else {
            for(YPathDesc yp : xunit.getYPaths()) {
                if( yp.getDimName().equals( ypDim)) {
                    return yp.getAttributeValue( ypAttr);
                }
            }
        }
        /// If we can't find it , return NULL
        return null;
    }


    @Override
    public String getDisplayString(String[] strings) {
        return "get_ypath_attribute( " + strings + " )";
    }
}
