package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 *  Return true if the string matches the Global XUnit /G
 */
@Description(
        name = "is_globel_xunit",
        value = "returns true if an XUnit matches the globel XUnit /G "
)
public class IsGlobalXUnitUDF extends UDF {

    public Boolean evaluate( String xunitStr) {
       //// Simply do a string compare
       ///   instead of parsing the YPath,
       ///  for efficiency sake
        if(xunitStr != null) {
            return xunitStr.equals(XUnitDesc.GlobalXUnitString);
        } else {
            return null;
        }
    }

}
