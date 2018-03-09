package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

/**
 *
 * Append a YPath to an XUnit
 *
 */
@Description(
        name = "append_ypath",
        value = "append a YPath to a given XUnit"
)
public class AppendYPathUDF extends GenericUDF {
    ObjectInspector xunitInspector = null;
    ObjectInspector ypathInspector = null;

	String usage ="append_ypath takes two arguments, an XUnit and a YPath";

	@Override
	public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
	   if( objectInspectors.length != 2) {
	      throw new UDFArgumentException( usage);
	   }
	   xunitInspector = XUnitUtils.ValidateXUnitObjectInspector(objectInspectors[0], usage);
       ypathInspector = XUnitUtils.ValidateYPathObjectInspector(objectInspectors[1], usage);


	   return XUnitUtils.GetXUnitInspector(); // return the named_struct  ; call print_xunit

	}

	@Override
	public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
	    XUnitDesc xunit = XUnitUtils.InspectXUnit( deferredObjects[0].get(), xunitInspector);
	    YPathDesc yp = XUnitUtils.InspectYPath( deferredObjects[1].get(), ypathInspector);


	    XUnitDesc appendedXUnit = xunit.appendYPath( yp);

	    return XUnitUtils.StructObjectForXUnit( appendedXUnit);
	}

	@Override
	public String getDisplayString(String[] strings) {
		return XUnitUtils.DisplayString("remove_yp_dim", strings);
	}
}
