package serializable;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by Thinkpads on 2018/3/15.
 */
public class SerialMain {
    private static final String TOPOLOGY_NAME = "test-serializable-topology";

    public static class SBolt extends BaseRichBolt {
        OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context,
                            OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            Customer customer = (Customer) tuple.getValueByField("customer");
            Integer id = tuple.getIntegerByField("id");
            System.out.println("customer name=" + customer.getName() + ";id=" + id);
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }

    public static class SSpout extends BaseRichSpout {
        public static Logger LOG = LoggerFactory.getLogger(SSpout.class);
        boolean _isDistributed;
        SpoutOutputCollector _collector;
        private Integer id = 0;

        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this._collector = collector;
        }

        public void close() {
        }

        public void nextTuple() {
            if (id > 4) {
                return;
            }

            Utils.sleep(100L);
            String[] names = new String[]{"tgor", "mike", "jackson", "mark", "sarah"};
            Random rand = new Random();

            id++;
            Customer customer = new Customer();
            customer.setAge(rand.nextInt(40));
            customer.setName(names[rand.nextInt(names.length)]);
            customer.setId(id);

            this._collector.emit(new Values(customer, id));
        }

        public void ack(Object msgId) {
        }

        public void fail(Object msgId) {
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(new String[]{"customer", "id"}));
        }
    }

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("serSpout", new SSpout(), 1);
        builder.setBolt("serBolt", new SBolt(), 1)
                .shuffleGrouping("serSpout");
        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());

        Utils.sleep(10000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }


}
