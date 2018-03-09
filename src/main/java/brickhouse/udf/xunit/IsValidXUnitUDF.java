package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 *  Return true if the string
 *    can be parsed into a valid XUnit.
 */
@Description(
        name = "is_valid_xunit",
        value = "returns true if a string can be parsed to a valid XUnit."
)
public class IsValidXUnitUDF extends UDF {

    public Boolean evaluate( String xunitStr) {
        try {
            if(xunitStr != null) {
                XUnitDesc xunit = XUnitDesc.ParseXUnit(xunitStr);
                return true;
            } else {
                return null;
            }
        } catch(IllegalArgumentException illArg) {

            System.err.println(" Unable to parse XUnit " + xunitStr);
            return false;
        }
    }

}
