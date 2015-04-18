package brickhouse.udf.timeseries;

import brickhouse.hbase.SaltedBigIntUDF;
import junit.framework.Assert;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TimeSeriesTest {

    static final String scoreListStr = "63.8413206534525,63.71369659407431,63.88648961452553,62.092462648245956,62.20327870946366,61.960318311195465,62.2674488939494,62.2524848049267,61.44278939756412,60.91723423709123,61.72830398640342,61.36357222476592,60.771082522833424,60.77169575698248,59.98175779182293,59.711860144622655,61.40788465211787,62.82350841867626,63.3773826786647,63.60533931399915,63.52152842519892,63.80639914193045,63.73289127601721,63.4391352669832,63.47419532674951,63.49170420334373,48.42479762818472,63.50089659914201,63.19736831007291,63.2833760692396,62.72605077957354,62.354773637227396,61.66826260457817,61.15105393281607,61.32862036910649,61.55738106428775,61.228762290188165,61.55738106428775,61.55738106428775,60.75356515947351,61.299350496423266,61.01404573748421,60.536572088412775,61.95696201175753";

    @Test
    public void testMovingAvgStr() throws UDFArgumentException {
        String[] scoreList = scoreListStr.split(",");
        System.out.println(" Score List size = " + scoreList.length);

        MovingAvgUDF mvnAvg = new MovingAvgUDF();
        mvnAvg.initialize(new ObjectInspector[]{
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector),
                PrimitiveObjectInspectorFactory.javaIntObjectInspector});

        List ts = Arrays.asList(scoreList);

        List<Double> avgList = mvnAvg.evaluate(ts, 3);

        for (int i = 0; i < avgList.size(); ++i) {
            System.out.println(" mvn Avg " + i + " == " + avgList.get(i));
        }

    }

    @Test
    public void testMovingAvgDouble() throws UDFArgumentException {
        String[] scoreList = scoreListStr.split(",");

        MovingAvgUDF mvnAvg = new MovingAvgUDF();
        mvnAvg.initialize(new ObjectInspector[]{
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector),
                PrimitiveObjectInspectorFactory.javaIntObjectInspector});


        List ts = new ArrayList<Double>();
        for (int i = 0; i < scoreList.length; ++i) {
            Double dblScore = Double.parseDouble(scoreList[i]);
            ts.add(dblScore);
        }

        List<Double> avgList = mvnAvg.evaluate(ts, 7);

        for (int i = 0; i < avgList.size(); ++i) {
            System.out.println(" mvn Avg " + i + " == " + avgList.get(i));
        }

    }

    @Test
    public void testSaltId() {
        SaltedBigIntUDF salty = new SaltedBigIntUDF();

        long joe = 44;
        String joeSalt = salty.evaluate(joe);

        System.out.println(" Joe's Salt = " + joeSalt);
        Assert.assertEquals("0044:44", joeSalt);

        long jerome = 995034;
        String jeromeSalt = salty.evaluate(jerome);

        System.out.println(" Jerome's Salt = " + jeromeSalt);
        Assert.assertEquals("5034:995034", jeromeSalt);

        long barack = 2055;
        String barackSalt = salty.evaluate(barack);

        System.out.println(" Barack's Salt = " + barackSalt);
        Assert.assertEquals("2055:2055", barackSalt);

    }

    @Test
    public void testMovingStdevStr() throws UDFArgumentException {
        String[] scoreList = scoreListStr.split(",");
        System.out.println(" Score List size = " + scoreList.length);

        MovingStdevUDF MovingStdev = new MovingStdevUDF();
        MovingStdev.initialize(new ObjectInspector[]{
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector),
                PrimitiveObjectInspectorFactory.javaIntObjectInspector});

        List ts = Arrays.asList(scoreList);

        List<Double> avgList = MovingStdev.evaluate(ts, 2);

        for (int i = 0; i < avgList.size(); ++i) {
            System.out.println(" rolling std dev " + i + " == " + avgList.get(i));
        }

    }

    @Test
    public void testMovingStdevDouble() throws UDFArgumentException {
        String[] scoreList = scoreListStr.split(",");

        MovingStdevUDF movingStdev = new MovingStdevUDF();
        movingStdev.initialize(new ObjectInspector[]{
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector),
                PrimitiveObjectInspectorFactory.javaIntObjectInspector});


        List ts = new ArrayList<Double>();
        for (int i = 0; i < scoreList.length; ++i) {
            Double dblScore = Double.parseDouble(scoreList[i]);
            ts.add(dblScore);
        }

        List<Double> avgList = movingStdev.evaluate(ts, 7);

        for (int i = 0; i < avgList.size(); ++i) {
            System.out.println(" rolling std dev " + i + " == " + avgList.get(i));
        }

    }
}
