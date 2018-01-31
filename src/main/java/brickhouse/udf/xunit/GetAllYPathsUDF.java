package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;

/**
 * Created by christopherleung on 1/30/18.
 */
public class GetAllYPathsUDF extends UDF {
    public String evaluate(String xunit) {
        String[] ypaths = xunit.split(",");

        ArrayList<String> ypathsList = new ArrayList<String>();

        for (int i = 0; i < ypaths.length; ++i) {
            String ypath = ypaths[i];
            ypathsList.add(ypath.substring(1, ypath.indexOf("/", 1)));
        }

        return String.join(",", ypathsList);
    }
}
