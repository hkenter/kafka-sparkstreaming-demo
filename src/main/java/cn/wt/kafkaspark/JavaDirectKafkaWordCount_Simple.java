package cn.wt.kafkaspark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class JavaDirectKafkaWordCount_Simple {
	public static void main(String[] args) {
		// if (args.length < 2) {
		// System.err.println("Usage: JavaDirectKafkaWordCount <brokers>
		// <topics>\n"
		// + " <brokers> is a list of one or more Kafka brokers\n"
		// + " <topics> is a list of one or more kafka topics to consume
		// from\n\n");
		// System.exit(1);
		// }

		String brokers = "218.61.208.41:9092";
		String topics = "test20180420";

		// Create context with a 2 seconds batch interval
		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("JavaDirectKafkaWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(3));

		HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);

		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

		// Get the lines, split them into words, count the words and print
		/**
		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" "));
			}
		});
		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		JavaPairDStream<String, Integer> wordsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		**/
		JavaDStream<String> lines = messages.map((tuple2) -> {System.out.println(tuple2._2);return tuple2._2();});
		JavaDStream<String> words = lines.flatMap(((line) -> {System.out.println(line);return Arrays.asList(line.split(" "));}));
		JavaPairDStream<String, Integer> pairs = words.mapToPair(((word) -> {System.out.println(word);return new Tuple2<String, Integer>(word, 1);}));
		JavaPairDStream<String, Integer> wordsCount = pairs.reduceByKey(((v1, v2) ->  v1 + v2 ));

		wordsCount.print();

		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}

}
