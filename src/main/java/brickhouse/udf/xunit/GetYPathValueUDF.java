package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;

/**
 * Created by christopherleung on 1/30/18.
 */
public class GetYPathValueUDF extends UDF {
    public String evaluate( String ypathPrefix, String xunit) {
        String[] ypaths = xunit.split(",");

        for (int i = 0; i < ypaths.length; ++i) {

            String ypath = ypaths[i];
            if (ypath.startsWith("/" + ypathPrefix)) {

                String nvpair = ypath.substring(ypath.indexOf("/", 1) + 1);

                if ("category".equals(ypathPrefix)) {
                    if (nvpair.contains("/")) {
                        String[] categoryLevels = nvpair.split("/");

                        String categoryTree = "";
                        for(String level : categoryLevels ) {

                            if (level.contains("=")) {
                                String[] nvpairSplit = level.split("=");

                                if (categoryTree.isEmpty()) {
                                    categoryTree = categoryTree + nvpairSplit[1];
                                } else {
                                    categoryTree = categoryTree + " > " + nvpairSplit[1];
                                }
                            }
                        }
                        return categoryTree.replaceAll("_", ",");
                    }
                } else {
                    if (nvpair.contains("=")) {
                        String[] nvpairSplit = nvpair.split("=");
                        return nvpairSplit[1].replaceAll("_", ",");
                    }
                }

            }
        }
        return null;
    }
}
