package config;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * Created by Thinkpads on 2018/3/15.
 */
public class ConfigMain {
    private static final String TOPOLOGY_NAME = "test-config-topology";

    public static class ConsoleBolt extends BaseRichBolt {
        OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context,
                            OutputCollector collector) {
            _collector = collector;
            Object obj = conf.get("test-conf");
            Object host = conf.get(ExampleConfig.HOST_NAME);
            System.out.println("test conf:" + obj);
            System.out.println("test conf obj host:" + host);
        }

        @Override
        public void execute(Tuple tuple) {
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        ExampleConfig exampleConfig = new ExampleConfig("127.0.0.1", 8099);

        builder.setSpout("wordSpout", new TestWordSpout(), 1);
        builder.setBolt("consoleBolt", new ConsoleBolt(), 1)
                .addConfiguration("test-conf", "conf example")
                .addConfigurations(exampleConfig.asMap())
                .shuffleGrouping("wordSpout");
        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());

        Utils.sleep(10000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }


}
