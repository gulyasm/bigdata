package hu.dmlab.spark;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;

public class SparkStreamingApplication implements Runnable, Serializable {

	public static void main(String[] args) {
		SparkStreamingApplication sparkStream = new SparkStreamingApplication();
		sparkStream.run();
	}

	public void run() {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(
				"NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				new Duration(1000));
		System.setProperty("twitter4j.oauth.consumerKey",
				"SdAaEy2NQxQsiVdqaoaNKhwNx");
		System.setProperty("twitter4j.oauth.consumerSecret",
				"rOMPFWLvlTXNPXRTAe9TJy1jEdupjd27TXZI2PGatAGSsrbSpN");
		System.setProperty("twitter4j.oauth.accessToken",
				"154070429-8PiYSmuniBiTuSeAl00YukhsBGzuu2DMb6LIpMvI");
		System.setProperty("twitter4j.oauth.accessTokenSecret",
				"DRXRaP0Jcaeruj8vbrwZJsny8EHO552YLZViyN1sfQYin");
		JavaReceiverInputDStream<Status> lines = TwitterUtils
				.createStream(jssc, new String[]{"bieber"});
		JavaDStream<String> words = lines
				.flatMap(new FlatMapFunction<Status, String>() {

					@Override
					public Iterable<String> call(Status arg0) throws Exception {
						return Arrays.asList(arg0.getText().split(" "));
					}
				});

		JavaPairDStream<String, Integer> pairs = words
				.mapToPair(new PairFunction<String, String, Integer>() {

					public Tuple2<String, Integer> call(String arg0)
							throws Exception {
						return new Tuple2<>(arg0, 1);
					}
				});

		JavaPairDStream<String, Integer> result = pairs
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					public Integer call(Integer arg0, Integer arg1)
							throws Exception {
						return arg0 + arg1;
					}
				});

		JavaPairDStream<String, Integer> filteredResult = result.filter(new Function<Tuple2<String,Integer>, Boolean>() {

			@Override
			public Boolean call(Tuple2<String, Integer> arg0) throws Exception {
				return arg0._2.intValue() > 2;
			}
		});

		filteredResult.print();

		jssc.start(); // Start the computation
		jssc.awaitTermination(); // Wait for the computation to terminate
	}
}
