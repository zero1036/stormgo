package mq;

import com.rabbitmq.client.ConnectionFactory;
import io.latent.storm.rabbitmq.*;
import io.latent.storm.rabbitmq.config.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.Scheme;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

/**
 * Created by Thinkpads on 2018/3/12.
 */
public class MqMain {
    private static final String TOPOLOGY_NAME = "test-rabbitmq-topology";

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        Scheme scheme = new RabbitMQMessageScheme(new MqScheme(), "envelope", "properties");
        IRichSpout spout = new RabbitMQSpout(scheme);

        ConnectionConfig connectionConfig = new ConnectionConfig(
                "localhost", 5672, "guest", "guest", ConnectionFactory.DEFAULT_VHOST, 10); // host, port, username, password, virtualHost, heartBeat
        ConsumerConfig spoutConfig = new ConsumerConfigBuilder().connection(connectionConfig)
                .queue("testExchange-q1")
                .prefetch(200)
                .requeueOnFail()
                .build();

        builder.setSpout("my-spout", spout)
                .addConfigurations(spoutConfig.asMap())
                .setMaxSpoutPending(200);

        builder.setBolt("my-bolt", new MyBolt(), 2).shuffleGrouping("my-spout");


        TupleToMessage msg = new TupleToMessageNonDynamic() {
            @Override
            protected byte[] extractBody(Tuple input) { return input.getStringByField("sendMsg").getBytes(); }
        };
        ProducerConfig sinkConfig = new ProducerConfigBuilder()
                .connection(connectionConfig)
                .contentEncoding("UTF-8")
                .contentType("application/json")
                .exchange("testExchangeOutput")
                .routingKey("")
                .build();
        builder.setBolt("send-msg-bolt", new RabbitMQBolt(msg))
                .addConfigurations(sinkConfig.asMap())
                .shuffleGrouping("my-bolt");

        Config config = new Config();//Config类是一个HashMap<String,Object>的子类，用来配置topology运行时的行为
        //设置worker数量
        //config.setNumWorkers(2);
        LocalCluster cluster = new LocalCluster();

        //本地提交
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());

        Utils.sleep(10000);
//        cluster.killTopology(TOPOLOGY_NAME);
//        cluster.shutdown();
    }
}
