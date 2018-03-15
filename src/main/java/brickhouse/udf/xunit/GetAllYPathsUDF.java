package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;

/**
 *
 * @Deprecated
 *
 * Use GetAllYPDims instead
 *
 */
@Deprecated
public class GetAllYPathsUDF extends UDF {
    ////public String[] evaluate(String xunit) {
     public String evaluate(String xunit) {
        if( ! xunit.equals( "/G")) {
            String[] ypaths = xunit.split(",");

            ArrayList<String> ypathsList = new ArrayList<String>();

            for (int i = 0; i < ypaths.length; ++i) {
                String ypath = ypaths[i];
                ypathsList.add(ypath.substring(1, ypath.indexOf("/", 1)));
            }

            ////return ypathsList.toArray(  new String[ ypathsList.size() ] );
           ///return String.join(",", ypathsList);
            StringBuilder sb = new StringBuilder( ypathsList.get( 0) );
            for(int i= 1; i <= ypathsList.size() -1 ; ++i) {
                sb.append(",");
                sb.append( ypathsList.get(i)) ;
            }

            return sb.toString();

        }  else {
            ///return  new String[ 0];
            return "";
        }
    }
}
