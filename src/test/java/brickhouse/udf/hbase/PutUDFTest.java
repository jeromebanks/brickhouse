package brickhouse.hbase;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

/**
 * NOTE: This is tester class not the unit-test.
 * This class contains tester class that runs only once TESTER_ON is set to true.
 */
public class PutUDFTest {
  static boolean TESTER_ON = false;

	@Test
	public void putTester() {
    if (!TESTER_ON) return;

    PutUDF udf = new PutUDF();
    Map<String, String> config = new HashMap<String, String>();
    config.put("table_name", "hive_metrics");
    config.put("hbase.zookeeper.quorum", "hbase3-zoo1,hbase3-zoo2,hbase3-zoo3");
    config.put("family", "c");
    config.put("qualifier", "q");
    // Write one value.
    System.out.println(udf.evaluate(config, "test.metrics.0", "2.0"));

    // Write map.
    Map<String, String> map = new HashMap<String, String>();
    map.put("test.metrics.1", "3.0");
    map.put("test.metrics.2", "1.0");
    System.out.println(udf.evaluate(config, map));
	}

}
