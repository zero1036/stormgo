package metric;

import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Thinkpads on 2018/3/13.
 */
public class ConsoleMetricsConsumer implements IMetricsConsumer {
    public static final Logger LOG = LoggerFactory.getLogger(LoggingMetricsConsumer.class);
    private static String padding = "                       ";

    public ConsoleMetricsConsumer() {
    }

    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) {
    }

    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        StringBuilder sb = new StringBuilder();
        String header = String.format("%s", "metric working:");
        sb.append(header);
        Iterator var5 = dataPoints.iterator();

        while (var5.hasNext()) {
            DataPoint p = (DataPoint) var5.next();
            sb.delete(header.length(), sb.length());
            sb.append(p.name).append(padding).delete(header.length() + 23, sb.length()).append("\t").append(p.value);
            LOG.info(sb.toString());
        }

    }

    public void cleanup() {
    }
}
