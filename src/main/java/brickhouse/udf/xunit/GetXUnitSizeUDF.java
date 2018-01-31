package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by christopherleung on 1/30/18.
 */
public class GetXUnitSizeUDF extends UDF{
    public Integer evaluate( String xunit) {
        return xunit.split(",").length;
    }
}
