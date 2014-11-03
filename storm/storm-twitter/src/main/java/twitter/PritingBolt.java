package twitter;

import java.text.MessageFormat;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class PritingBolt extends BaseBasicBolt {

    private static final int THRESHOLD = 5;

    @Override
    public void cleanup() {
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Integer count = input.getInteger(1);
        if (count > THRESHOLD) {
            String link = MessageFormat.format("{0} - {1}", input.getString(0), count);
            System.out.println(link);
        }

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
