package tn.insat.tp22;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;


public class NetworkWordCount {
    public static void main(String[] args) throws InterruptedException {

        String localhost = "localhost";
        String host = "172.18.0.1";

        SparkConf conf = new SparkConf()
                .setAppName("NetworkWordCount")
                .setMaster("local");
        JavaStreamingContext jssc =
                new JavaStreamingContext(conf, Durations.seconds(1));

        JavaReceiverInputDStream<String> lines =
                jssc.socketTextStream(host, 9999);

        JavaDStream<String> words =
                lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairs =
                words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts =
                pairs.reduceByKey((i1, i2) -> i1 + i2);

        wordCounts.foreachRDD(rdd -> rdd.foreach(data -> System.out.println(data._1+"-----"+data._2)));

        jssc.start();
        jssc.awaitTermination();
    }
}
