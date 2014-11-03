package hu.dmlab.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordCounterApplication {

    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());
        builder.setBolt("word-normalizer", new WordCountBolt()).shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCountCounterBolt(), 4).fieldsGrouping("word-normalizer", new Fields("word"));
        // Configuration
        Config conf = new Config();
        conf.setDebug(false);
        // Topology run
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("WordCount Storm example app", conf, builder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();
    }
}
