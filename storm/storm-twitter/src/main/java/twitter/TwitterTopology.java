package twitter;

import twitter.LinkExtractorBolt;
import twitter.PritingBolt;
import twitter.TrendBolt;
import twitter.TwitterSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class TwitterTopology {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("tweets-collector", new TwitterSpout());
        builder.setBolt("urlemitter", new LinkExtractorBolt()).shuffleGrouping("tweets-collector");
        builder.setBolt("urlcounter", new TrendBolt()).fieldsGrouping("urlemitter", new Fields("tweet-link"));
        builder.setBolt("urlcounter-printer", new PritingBolt()).shuffleGrouping("urlcounter");
        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();
        cluster.submitTopology("twitter-test", conf, builder.createTopology());
    }

}
