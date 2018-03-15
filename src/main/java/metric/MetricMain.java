package metric;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.metric.api.ReducedMetric;
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

import java.util.Map;

/**
 * MetricMain
 * Created by Thinkpads on 2018/3/13.
 */
public class MetricMain {
    public static class ExclamationBolt extends BaseRichBolt {
        OutputCollector _collector;
        // 定义指标统计对象
        transient CountMetric _countMetric;
        transient MultiCountMetric _wordCountMetric;
        transient ReducedMetric _wordLengthMeanMetric;

        @Override
        public void prepare(Map conf, TopologyContext context,
                            OutputCollector collector) {
            _collector = collector;
            initMetrics(context);
        }

        @Override
        public void execute(Tuple tuple) {
            _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
            _collector.ack(tuple);
            updateMetrics(tuple.getString(0));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        // 初始化计数器
        void initMetrics(TopologyContext context) {
            _countMetric = new CountMetric();
            _wordCountMetric = new MultiCountMetric();
            _wordLengthMeanMetric = new ReducedMetric(new MeanReducer());
            context.registerMetric("execute_count", _countMetric, 5);
            context.registerMetric("word_count", _wordCountMetric, 5);
            context.registerMetric("word_length", _wordLengthMeanMetric, 5);
        }

        // 更新计数器
        void updateMetrics(String word) {
            _countMetric.incr();
            _wordCountMetric.scope(word).incr();
            _wordLengthMeanMetric.update(word.length());
        }
    }

    public static class MetricSpout extends BaseRichSpout {

        //BaseRichSpout是ISpout接口和IComponent接口的简单实现，接口对用不到的方法提供了默认的实现

        private SpoutOutputCollector collector;
        private String[] sentences = {
                "a", "b", "cc", "ddd"
        };

        private int index = 0;

        /**
         * open()方法中是ISpout接口中定义，在Spout组件初始化时被调用。
         * open()接受三个参数:一个包含Storm配置的Map,一个TopologyContext对象，提供了topology中组件的信息,SpoutOutputCollector对象提供发射tuple的方法。
         * 在这个例子中,我们不需要执行初始化,只是简单的存储在一个SpoutOutputCollector实例变量。
         */
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        /**
         * nextTuple()方法是任何Spout实现的核心。
         * Storm调用这个方法，向输出的collector发出tuple。
         */
        public void nextTuple() {
            if (index >= sentences.length) {
//            index = 0;
                Utils.sleep(1000);
                return;
            } else {
                this.collector.emit(new Values(sentences[index]), index);
                index++;
            }
            Utils.sleep(1);
        }

        /**
         * declareOutputFields是在IComponent接口中定义的，所有Storm的组件（spout和bolt）都必须实现这个接口
         * 用于告诉Storm流组件将会发出那些数据流，每个流的tuple将包含的字段
         */
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));//告诉组件发出数据流包含sentence字段
        }
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word", new MetricSpout(), 1);
        builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping(
                "word");
//        builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping(
//                "exclaim1");
        Config conf = new Config();
        conf.setDebug(true);
        // 输出统计指标值到日志文件中
        conf.registerMetricsConsumer(ConsoleMetricsConsumer.class, 2);
//        if (args != null && args.length > 0) {
//            conf.setNumWorkers(3);
//            StormSubmitter.submitTopology(args[0], conf,
//                    builder.createTopology());
//        } else {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("testMetric", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.killTopology("testMetric");
        cluster.shutdown();
//        }
    }
}
