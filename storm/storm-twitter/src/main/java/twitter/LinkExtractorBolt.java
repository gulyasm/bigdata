package twitter;

import java.util.Map;

import twitter4j.Status;
import twitter4j.URLEntity;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class LinkExtractorBolt extends BaseBasicBolt {

    @Override
    public void cleanup() {
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Status status = (Status) input.getValueByField("tweet");
        for (URLEntity urlentity : status.getURLEntities()) {
            collector.emit(new Values(urlentity.getDisplayURL()));
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet-link"));
    }

}
