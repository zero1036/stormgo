import bolts.*;
import org.apache.storm.tuple.Fields;
import spouts.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Created by Thinkpads on 2018/3/8.
 */
public class TopoMain {

    private static final String SENTENCE_SPOUT_ID = "test-spout";
    private static final String FIRST_BOLT_ID = "test-bolt";
    private static final String SECOND_BOLT_ID = "second-bolt";
    private static final String THIRD_BOLT_ID = "third-bolt";
    private static final String TOPOLOGY_NAME = "test-count-topology";

    public static void main(String[] args) //throws Exception
    {
        //System.out.println( "Hello World!" );
        //实例化spout和bolt

        TestSpout spout = new TestSpout();
        TestFirstBolt firstBolt = new TestFirstBolt();
        TestSecondBolt secondBolt = new TestSecondBolt();
        TestThirdBolt thirdBolt = new TestThirdBolt();

        TopologyBuilder builder = new TopologyBuilder();//创建了一个TopologyBuilder实例

        //TopologyBuilder提供流式风格的API来定义topology组件之间的数据流

        //builder.setSpout(SENTENCE_SPOUT_ID, spout);//注册一个sentence spout

        //设置两个Executeor(线程)，默认一个
        builder.setSpout(SENTENCE_SPOUT_ID, spout, 1);

        // SentenceSpout --> SplitSentenceBolt

        //注册一个bolt并订阅sentence发射出的数据流，shuffleGrouping方法告诉Storm要将SentenceSpout发射的tuple随机均匀的分发给SplitSentenceBolt的实例
        //builder.setBolt(FIRST_BOLT_ID, splitBolt).shuffleGrouping(SENTENCE_SPOUT_ID);

        builder.setBolt(FIRST_BOLT_ID, firstBolt, 2).shuffleGrouping(SENTENCE_SPOUT_ID);

        builder.setBolt(SECOND_BOLT_ID, secondBolt, 2).fieldsGrouping(FIRST_BOLT_ID, new Fields("firstGroup"));

        //globalGrouping是把WordCountBolt发射的所有tuple路由到唯一的ReportBolt
        builder.setBolt(THIRD_BOLT_ID, thirdBolt, 2).globalGrouping(SECOND_BOLT_ID);

        // SplitSentenceBolt --> WordCountBolt

        Config config = new Config();//Config类是一个HashMap<String,Object>的子类，用来配置topology运行时的行为
        //设置worker数量
        //config.setNumWorkers(2);
        LocalCluster cluster = new LocalCluster();

        //本地提交
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());

        Utils.sleep(10000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();

    }
}
