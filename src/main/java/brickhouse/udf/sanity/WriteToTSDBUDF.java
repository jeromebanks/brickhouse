package brickhouse.udf.sanity;
/**
 * Copyright 2012 Klout, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/

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
 *
 */
@Description(
        name = "write_to_tsdb",
        value = " This function writes metrics to the TSDB  (metics names should look like proc.loadavg.1min, http.hits " +
                "   while tags string is space separated collection of tags). On failiure returns 'WRITE_FAILED' otherwise \n" +
                "   'WRITE_OK' \n " +
                "_FUNC_(String hostname, int port, Map<String, Double> nameToValue, String tags, Long timestampInSeconds) \n" +
                "_FUNC_(String hostname, int port, Map<String, Double> nameToValue, String tags) \n" +
                "_FUNC_(String hostname, int port, Map<String, Double> nameToValue) \n" +
                "_FUNC_(String hostname, int port, String metricName, Double metricVaule, String tags, Long timestampInSeconds) \n" +
                "_FUNC_(String hostname, int port, String metricName, Double metricVaule, String tags) \n" +
                "_FUNC_(String hostname, int port, String metricName, Double metricVaule) \n"

)
public class WriteToTSDBUDF extends UDF {
    TSDBWriter writer = new TSDBWriter();

    // TODO(nemanja) - refactor this to the general tool.
    class TSDBWriter {
        public void sendData(String hostname, int port, String data) throws IOException {
            InetAddress ia = InetAddress.getByName(hostname);
            Socket socket = new Socket(ia, port);
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            out.write(data);
            out.flush();
            out.close();
            socket.close();
        }

        public void sendMetrics(String hostname, int port, String metric, Double value, String tags, Long timeInSeconds) throws IOException {
            Map<String, Double> metricsToValue = new HashMap<String, Double>();
            metricsToValue.put(metric, value);
            sendMetrics(hostname, port, metricsToValue, tags, timeInSeconds);
        }

        public void sendMetrics(
                String hostname, int port, Map<String, Double> nameToValue, String tags, Long timeInSeconds) throws IOException {
            if (timeInSeconds == null) {
                timeInSeconds = new DateTime().getMillis() / 1000;
            }
            String localHostName = InetAddress.getLocalHost().getHostName();
            StringBuffer ouputString = new StringBuffer();
            for (String metric : nameToValue.keySet()) {
                ouputString.append(String.format("put %s %d %f host=%s %s\n",
                        metric, timeInSeconds, nameToValue.get(metric), localHostName, tags));
            }
            sendData(hostname, port, ouputString.toString());
        }
    }


    public String evaluate(String host, Integer port, Map<String, Double> nameToValue, String tags, Long timestampInSeconds) {
        if (host.equals("") || port < 0) {
            return "WRITE_FAILED";
        }
        try {
            writer.sendMetrics(host, port, nameToValue, tags, timestampInSeconds);
        } catch (IOException e) {
            e.printStackTrace();
            return "WRITE_FAILED";
        }
        return "WRITE_OK";
    }

    public String evaluate(String host, Integer port, Map<String, Double> nameToValue, String tags) {
        return evaluate(host, port, nameToValue, tags, null);
    }

    public String evaluate(String host, Integer port, Map<String, Double> nameToValue) {
        return evaluate(host, port, nameToValue, null);
    }


    // Non Map input.

    public String evaluate(String host, Integer port, String name, Double value, String tags, Long timestampInSeconds) {
        Map<String, Double> nameToValue = new HashMap<String, Double>();
        nameToValue.put(name, value);
        return evaluate(host, port, nameToValue, tags, timestampInSeconds);
    }

    public String evaluate(String host, Integer port, String name, Double value, String tags) {
        return evaluate(host, port, name, value, tags, null);
    }

    public String evaluate(String host, Integer port, String name, Double value) {
        return evaluate(host, port, name, value, null);
    }

}
