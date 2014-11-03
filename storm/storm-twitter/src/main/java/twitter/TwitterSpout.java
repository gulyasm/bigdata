package twitter;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.json.simple.parser.JSONParser;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TwitterSpout extends BaseRichSpout implements StatusListener {
    static String                       STREAMING_API_URL = "https://stream.twitter.com/1/statuses/filter.json?track=";
    static Logger                       LOG               = Logger.getLogger(TwitterSpout.class);
    static JSONParser                   jsonParser        = new JSONParser();
    private LinkedBlockingQueue<Status> queue             = null;
    private SpoutOutputCollector        collector;
    int                                 currentIndex      = 0;

    @Override
    public void nextTuple() {
        final ArrayList<Status> tweets = new ArrayList<>();
        queue.drainTo(tweets);
        for (Status tweet : tweets) {
            collector.emit(new Values(tweet));
        }

    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        queue = new LinkedBlockingQueue<>(1024 * 1024);
        this.collector = collector;
        ConfigurationBuilder builder = new ConfigurationBuilder();
        TwitterStream twitterStream = new TwitterStreamFactory(builder.build()).getInstance();
        twitterStream.setOAuthConsumer(
                "SdAaEy2NQxQsiVdqaoaNKhwNx",
                    "rOMPFWLvlTXNPXRTAe9TJy1jEdupjd27TXZI2PGatAGSsrbSpN");
        AccessToken token = new AccessToken(
                "154070429-8PiYSmuniBiTuSeAl00YukhsBGzuu2DMb6LIpMvI",
                "DRXRaP0Jcaeruj8vbrwZJsny8EHO552YLZViyN1sfQYin");
        twitterStream.setOAuthAccessToken(token);
        twitterStream.addListener(this);
        twitterStream.sample();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

    @Override
    public void onException(Exception arg0) {
        System.out.println("onException: " + arg0);
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice arg0) {
    }

    @Override
    public void onScrubGeo(long arg0, long arg1) {
    }

    @Override
    public void onStallWarning(StallWarning arg0) {
    }

    @Override
    public void onStatus(Status arg0) {
        queue.offer(arg0);
    }

    @Override
    public void onTrackLimitationNotice(int arg0) {
    }

}
