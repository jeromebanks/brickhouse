package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 *  Return true if the string matches the Global XUnit /G
 */
public class IsGlobalXUnitUDF extends UDF {

    public Boolean evaluate( String xunitStr) {
       //// Simply do a string compare
       ///   instead of parsing the YPath,
       ///  for efficiency sake
       return xunitStr.equals( XUnitDesc.GlobalXUnitString);
    }

}
