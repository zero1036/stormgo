
import tree.bolts.CountBolt;
import tree.bolts.MergeBolt;
import tree.bolts.ReportBolt;
import tree.bolts.SplitBolt;
import tree.spouts.TreeSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Created by Thinkpads on 2018/3/8.
 */
public class TreeMain {

    private static final String SENTENCE_SPOUT_ID = "test-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String MERGE_BOLT_ID = "merge-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "test-count-topology";

    public static void main(String[] args) //throws Exception
    {
        //System.out.println( "Hello World!" );
        //实例化spout和bolt

        TreeSpout spout = new TreeSpout();
        SplitBolt splitBolt = new SplitBolt();
        CountBolt countBolt = new CountBolt();
        MergeBolt mergeBolt = new MergeBolt();
        ReportBolt reportBolt = new ReportBolt();

        TopologyBuilder builder = new TopologyBuilder();//创建了一个TopologyBuilder实例

        //TopologyBuilder提供流式风格的API来定义topology组件之间的数据流

        //builder.setSpout(SENTENCE_SPOUT_ID, spout);//注册一个sentence spout

        //设置两个Executeor(线程)，默认一个
        builder.setSpout(SENTENCE_SPOUT_ID, spout, 1);

        // SentenceSpout --> SplitSentenceBolt

        //注册一个bolt并订阅sentence发射出的数据流，shuffleGrouping方法告诉Storm要将SentenceSpout发射的tuple随机均匀的分发给SplitSentenceBolt的实例
        //builder.setBolt(FIRST_BOLT_ID, splitBolt).shuffleGrouping(SENTENCE_SPOUT_ID);

        builder.setBolt(SPLIT_BOLT_ID, splitBolt, 1).shuffleGrouping(SENTENCE_SPOUT_ID);

        builder.setBolt(COUNT_BOLT_ID, countBolt, 1).shuffleGrouping(SPLIT_BOLT_ID);
        builder.setBolt(MERGE_BOLT_ID, mergeBolt, 1).shuffleGrouping(SPLIT_BOLT_ID);

        //globalGrouping是把WordCountBolt发射的所有tuple路由到唯一的ReportBolt
        builder.setBolt(REPORT_BOLT_ID, reportBolt, 1)
                .shuffleGrouping(COUNT_BOLT_ID)
                .shuffleGrouping(MERGE_BOLT_ID);

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
