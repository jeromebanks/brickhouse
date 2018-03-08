package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.ArrayList;
import java.util.List;

/**
 *  Return an XUnit with a specific 
 *    YPath dimension removed.
 *
 */
public class RemoveYPathUDF extends GenericUDF {
    ObjectInspector xunitInspector = null;
    StringObjectInspector ypDimInspector = null;

	String usage ="remove_yp_dim takes two arguments, an XUnit and a YPath dimension";

	@Override
	public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
	   if( objectInspectors.length != 2) {
	      throw new UDFArgumentException( usage);
	   }
	   xunitInspector = XUnitUtils.ValidateXUnitObjectInspector(objectInspectors[0], usage);
	   if( !(objectInspectors[1] instanceof StringObjectInspector)) {
           throw new UDFArgumentException( usage);
       }
       ypDimInspector = (StringObjectInspector)objectInspectors[1];


	   return XUnitUtils.GetXUnitInspector(); // return the named_struct

	}

	@Override
	public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
	    XUnitDesc xunit = XUnitUtils.InspectXUnit( deferredObjects[0].get(), ypDimInspector);
	    String ypDim = ypDimInspector.getPrimitiveJavaObject( deferredObjects[1].get());

	    if(xunit.isGlobal()) {
	      return XUnitUtils.StructObjectForXUnit( xunit);
        } else {
          XUnitDesc removed = xunit.removeYPath( ypDim);
          return XUnitUtils.StructObjectForXUnit(removed);
        }
	}

	@Override
	public String getDisplayString(String[] strings) {
		return XUnitUtils.DisplayString("remove_yp_dim", strings);
	}
}
