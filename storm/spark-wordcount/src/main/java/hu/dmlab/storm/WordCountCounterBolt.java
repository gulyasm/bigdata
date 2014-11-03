package hu.dmlab.storm;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class WordCountCounterBolt implements IRichBolt {
    private Map<String, Integer> counters;
    private OutputCollector      collector;
    String                       name;
    Integer                      id;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.counters = new HashMap<String, Integer>();
        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();

    }

    public void execute(Tuple input) {
        String str = input.getString(0);
        if (!counters.containsKey(str)) {
            counters.put(str, 1);
        } else {
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }
        // Set the tuple as Acknowledge
        collector.ack(input);

    }

    public void cleanup() {
        System.out.println("===== WordCount result =====");
        for (Map.Entry<String, Integer> entry : counters.entrySet()) {
            System.out.println(MessageFormat.format("{0};{1}", entry.getKey(), entry.getValue()));
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
