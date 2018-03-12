package tree.bolts;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * 订阅sentence spout发射的tuple流，实现筛选不同长度的单词
 *
 * @author soul
 */
public class SplitBolt extends BaseRichBolt {
    //BaseRichBolt是IComponent和IBolt接口的实现
    //继承这个类，就不用去实现本例不关心的方法

    private OutputCollector collector;

    /**
     * prepare()方法类似于ISpout 的open()方法。
     * 这个方法在blot初始化时调用，可以用来准备bolt用到的资源,比如数据库连接。
     * 本例子和SentenceSpout类一样,SplitSentenceBolt类不需要太多额外的初始化,
     * 所以prepare()方法只保存OutputCollector对象的引用。
     */
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * 核心功能是在类IBolt定义execute()方法，这个方法是IBolt接口中定义。
     * 每次Bolt从流接收一个订阅的tuple，都会调用这个方法。
     * 本例中,收到的元组中实现筛选不同长度的单词
     * 然后将单词长度及单词本身生成新的tuple并发出。
     */
    public void execute(Tuple input) {
        String sentence = input.getStringByField("sentence");
        System.out.println("bolt1 is workding:" + sentence);

        this.collector.emit(input, new Values(sentence));//向下一个bolt发射数据
        this.collector.ack(input);
    }

    /**
     * 定义一个元组流,每个包含一个字段("firstGroup", "firstValue")。
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

}
