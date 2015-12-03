package brickhouse.udf.xunit;
import java.util.Comparator;

/**
 * Created by janandaram on 12/3/15.
 */
public class YPathDescComparator implements Comparator<YPathDesc> {
    @Override
    public int compare(YPathDesc yp1, YPathDesc yp2) {
        return yp1.getDimName().compareTo(yp2.getDimName());
    }
}
