package dependable.bolts;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * @author soul
 */
public class TestSecondBolt extends BaseRichBolt {
    //BaseRichBolt是IComponent和IBolt接口的实现
    //继承这个类，就不用去实现本例不关心的方法

    private OutputCollector collector;

    /**
     * prepare()方法类似于ISpout 的open()方法。
     * 这个方法在blot初始化时调用，可以用来准备bolt用到的资源,比如数据库连接。
     * 所以prepare()方法只保存OutputCollector对象的引用。
     */
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * 核心功能：分组长度为1的单词后添加*号作为新的tuple并发出；分组长度为2的单词不处理直接作为tuple发出
     */
    public void execute(Tuple input) {
        Integer group = input.getIntegerByField("firstGroup");
        String value = input.getStringByField("firstValue");
        System.out.println("bolt2 is workding:" + value);

        if (group == 1) {
            value = String.format("%s*", value);
        }
        this.collector.emit(input, new Values(value));

//        ack全部情况
//        this.collector.ack(input);

        //ack 分组为1的元组，fail分组为2的元组
        if (group == 1) {
            this.collector.ack(input);
        } else {
            this.collector.fail(input);
        }
    }

    /**
     * plitSentenceBolt类定义一个元组流,每个包含一个字段(“secondValue”)。
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("secondValue"));
    }

}
