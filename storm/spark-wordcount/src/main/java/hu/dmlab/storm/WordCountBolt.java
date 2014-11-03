package hu.dmlab.storm;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountBolt implements IRichBolt {

    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    /*
     * (non-Javadoc)
     * 
     * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
     * 
     * It's called once per tuple
     */
    public void execute(Tuple input) {
        String line = input.getString(0);
        String[] split = line.replaceAll("[^A-Za-z 0-9]", "").toLowerCase().split("\\s+");
        for (String word : split) {
            word = word.trim().toLowerCase();
            collector.emit(new Values(word));
            System.out.println("Emmitting: " + word);
        }
        collector.ack(input);

    }

    public void cleanup() {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

}
