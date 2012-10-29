package brickhouse.udf.sanity;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.DateTime;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

/**
 * Author: Nemanja Spasojevic (nemanja@klout.com)
 *
 *   UDF for writing into the graphite.
 */
@Description(
 name = "write_to_graphite",
 value =  "  \n " +
          "_FUNC_(String hostname, int port, Map<String, Double> nameToValue, Long timestampInSeconds) \n" +
          "_FUNC_(String hostname, int port, Map<String, Double> nameToValue) \n" +
          "_FUNC_(String hostname, int port, String metricName, Double metricVaule, String tags, Long timestampInSeconds) \n" +
          "_FUNC_(String hostname, int port, String metricName, Double metricVaule, String tags) \n"
)
public class WriteToGraphiteUDF extends UDF {
  GraphiteWriter writer_ = new GraphiteWriter();

  /**
   * Utility writer containing logic to write to the graphite.
   *
   * TODO(nemanja) - refactor this to the general tool.
   */
  class GraphiteWriter {
    public void sendData(String hostname, int port, String data) throws IOException {
      InetAddress ia = InetAddress.getByName(hostname);
      Socket socket = new Socket(ia, port);
      BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
      out.write(data);
      out.flush();
      out.close();
      socket.close();
    }

    public void sendMetrics(String hostname, int port, String metric, Double value, Long timeInSeconds) throws IOException {
      Map<String, Double> metricsToValue = new HashMap<String, Double>();
      metricsToValue.put(metric, value);
      sendMetrics(hostname, port, metricsToValue, timeInSeconds);
    }

    public void sendMetrics(
        String hostname, int port, Map<String, Double> nameToValue, Long timeInSeconds) throws IOException {
      if (timeInSeconds == null) {
        timeInSeconds = new DateTime().getMillis() / 1000;
      }
      StringBuffer ouputString = new StringBuffer();
      for (String metric : nameToValue.keySet()) {
        ouputString.append(String.format("%s %f %d\n",
            metric, nameToValue.get(metric), timeInSeconds));
      }
      sendData(hostname, port, ouputString.toString());
    }
  }


  public String evaluate(String host, Integer port, Map<String, Double> nameToValue, Long timestampInSeconds) {
    if (host.equals("") || port < 0) {
      return "WRITE_FAILED";
    }
    try {
      writer_.sendMetrics(host, port, nameToValue, timestampInSeconds);
    } catch (IOException e) {
      e.printStackTrace();
      return "WRITE_FAILED";
    }
    return "WRITE_OK";
  }

  public String evaluate(String host, Integer port,  Map<String, Double> nameToValue) {
    return evaluate(host, port, nameToValue, null);
  }

  // Non Map input.
  
  public String evaluate(String host, Integer port, String name, Double value, Long timestampInSeconds) {
    Map<String, Double> nameToValue = new HashMap<String, Double>();
    nameToValue.put(name, value);
    return evaluate(host, port, nameToValue, timestampInSeconds);
  }

  public String evaluate(String host, Integer port, String name, Double value) {
    return evaluate(host, port, name, value, null);
  }

}
