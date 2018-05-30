package cn.wt.kafkaspark.demo;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class SparkStreamingNoKafka {

	public static void main(String[] args) {
		/**
		 * local模式的话,local后必须为大于等于2的数字【即至少2条线程】。因为receiver 占了一个。
		 * 因为,SparkStreaming 在运行的时候,至少需要一条线程不断循环接收数据，而且至少有一条线程处理接收的数据。
		 * 如果只有一条线程的话,接受的数据不能被处理。
		 */
		// 注意本地调试，master必须为local[n],n>1,表示一个线程接收数据，n-1个线程处理数据
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("demo");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// 设置日志运行级别
		sc.setLogLevel("WARN");
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(5));
		// 创建Spark Streaming 输入数据来源：input Stream
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream("218.61.208.42", 9999);
		// // Split each line into words
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

		/**
		 * 此处的print并不会直接触发Job的执行，因为现在的一切都是在Spark Streaming框架的控制之下的。
		 * 具体是否真正触发Job的运行是基于设置的Duration时间间隔的触发的。
		 * 
		 * 
		 * Spark应用程序要想执行具体的Job对DStream就必须有output Stream操作。 ouput Stream
		 * 有很多类型的函数触发,类print、savaAsTextFile、saveAsHadoopFiles等，最为重要的是foreachRDD，
		 * 因为Spark Streamimg处理的结果一般都会 放在Redis、DB、DashBoard等上面，foreachRDD主要 就是用来
		 * 完成这些功能的，而且可以随意的自定义具体数据放在哪里。
		 * 
		 */
		wordsCount.print();

		ssc.start();
		ssc.awaitTermination();
		ssc.close();

	}
	
}
