package org.kratos.spark.batch;

import java.util.Arrays;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.kratos.spark.applicationContext.ApplicationContext;

import scala.Tuple2;

public class SparkStreamingJob {

	public static void main(String[] args) throws InterruptedException {

		ApplicationContext app = ApplicationContext.getInstance();
		JavaStreamingContext jssc = app.getStreamingContext(Durations.seconds(5));

		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
		// Split each line into words
		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

		// Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

		JavaPairDStream<String, Integer> windowedWordCounts = pairs.reduceByKeyAndWindow((i1, i2) -> i1 + i2, Durations.seconds(30), Durations.seconds(15));

		// Print the first ten elements of each RDD generated in this DStream to the console
		wordCounts.print();

		windowedWordCounts.print();

		jssc.start(); // Start the computation
		jssc.awaitTermination(); // Wait for the computation to terminate

	}

}
