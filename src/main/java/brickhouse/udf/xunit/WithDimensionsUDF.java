package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.List;

/**
 *
 * Return true only if the given XUnit has
 *   a specified set of dimensions
 *
 */
@Description(
        name="with_ypath_dimensions",
        value="_FUNC_(string, array<string>) - ",
        extended="return true if and only if the xunit contains only the specified dimensions")
public class WithDimensionsUDF extends UDF {

    public Boolean evaluate( String xunitStr, List<String> dims) {
       XUnitDesc xunit = XUnitDesc.ParseXUnit( xunitStr);
       if( xunit.numDims() != dims.size() ) {
           return false;
       }

       for( YPathDesc yp : xunit.getYPaths()) {
           if( ! dims.contains( yp.getDimName())) {
               return false;
           }
       }
       return true;
    }

}
