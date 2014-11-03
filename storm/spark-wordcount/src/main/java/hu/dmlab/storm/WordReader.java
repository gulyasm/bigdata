package hu.dmlab.storm;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WordReader implements IRichSpout {

    private SpoutOutputCollector collector;
    private FileReader           fileReader;
    private boolean              completed = false;
    private TopologyContext      context;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        try {
            this.fileReader = new FileReader(
                    "/home/gulyasm/panopticon/big-data-course/examples/storm/spark-wordcount/war_peace_text");
            this.context = context;
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file!", e);
        }
        this.collector = collector;

    }

    public void close() {
        // TODO Auto-generated method stub

    }

    public void nextTuple() {
        if (completed) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
        String str = null;
        BufferedReader reader = new BufferedReader(fileReader);
        try {
            while ((str = reader.readLine()) != null) {
                collector.emit(new Values(str), str);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading tuple", e);
        } finally {
            completed = true;
        }

    }

    public void ack(Object msgId) {
    }

    public void fail(Object msgId) {
        System.out.println(MessageFormat.format("FAILED={0}", msgId));

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));

    }

    public boolean isDistributed() {
        return false;
    }

}
