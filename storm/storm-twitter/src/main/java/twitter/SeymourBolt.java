package twitter;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class SeymourBolt implements IRichBolt {

    private OutputCollector collector;
    int                     seymourCounter = 0;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        TimerTask task = new TimerTask() {

            @Override
            public void run() {
                final int seymourtweets = seymourCounter;
                seymourCounter = 0;
                StringBuilder builder = new StringBuilder();
                builder.append("Seymour tweets: ").append(seymourtweets);
                System.out.println(builder.toString());
            }
        };
        Timer t = new Timer();
        t.scheduleAtFixedRate(task, 5000, 5000);

    }

    @Override
    public void execute(Tuple input) {
        String content = input.getStringByField("tweet").toLowerCase();
        if (content.contains("seymour")) {
            seymourCounter++;
        }

    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
