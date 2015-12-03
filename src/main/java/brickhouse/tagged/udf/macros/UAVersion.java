package tagged.udf.macros;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by janandaram on 10/27/15.
 */
public class UAVersion extends UDF {

    public String evaluate(String userAgent) {
        if(userAgent == null) return "Unknown";
        String lowUserAgent = userAgent.toLowerCase();
        if(((lowUserAgent.contains("tagged") || lowUserAgent.contains("hi5")) &&
                lowUserAgent.contains("/an")) || lowUserAgent.contains("dalvik"))
            return "Android";

        else if((lowUserAgent.contains("tagged") || lowUserAgent.contains("hi5")) &&
                lowUserAgent.contains("darwin"))
            return "iOS";
        else if (!lowUserAgent.contains("tagged") && !lowUserAgent.contains("hi5") &&
                (lowUserAgent.contains("mobi") || lowUserAgent.contains("android") ||
                lowUserAgent.contains("android") || lowUserAgent.contains("blackberry") ||
                        lowUserAgent.contains("nokia") || lowUserAgent.contains("samsung") ||
                        lowUserAgent.contains("iphone") || lowUserAgent.contains("opera") ))
            return "Mobile Web";
        else if (lowUserAgent != null && lowUserAgent.length() > 1 )
            return "Desktop Web";
        else return "Unknown";

    }

}
