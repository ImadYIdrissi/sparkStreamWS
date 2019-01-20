package com.akrivis.cb1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

public class TwitterMostPopularHashtags {

    public static void main(String[] args) throws InterruptedException, IOException {
        twitterSetup();
        SparkConf conf = new SparkConf()
                .setAppName("PopularHashtags")
                .setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaReceiverInputDStream<Status> tweets = TwitterUtils.createStream(jssc);

        // Get the texts from the tweets.
        JavaDStream<String> tweetTexts = tweets.map(s -> s.getText());
        // Resulting tweetTexts RDD : ('Textual_information_1',
        //                             'Textual_information_2',...)

        // Get the words from each tweetText
        JavaDStream<String> tweetWords = tweetTexts.flatMap(t -> Arrays.asList(t.split(" ")).iterator());
        // Resulting tweetWords RDD : ('word_1',
        //                             'word_2',...)

        // Filter the words, keep the hashtags only
        JavaDStream<String> tweetHashtags = tweetWords.filter(w -> w.startsWith("#"));
        // Resulting tweetWords RDD : ('#word_1',
        //                             '#word_2',...)

        // Pairing each hashtag with 1
        JavaPairDStream<String, Integer> tweetHashtags_v = tweetHashtags.mapToPair(t -> new Tuple2<>(t, 1));
        // Resulting tweetWords RDD : (('#word_1',1),
        //                             ('#word_2',1), ...)

        // Reducing by key : hashtag word, and by window.
        JavaPairDStream<String, Integer> hashtagCount = tweetHashtags_v.reduceByKeyAndWindow((a, b) -> a + b, (a, b) -> a - b, Durations.seconds(300), Durations.seconds(1));
        // Resulting hashtagCount RDD : (('#word_1', n),
        //                               ('#word_2', m), ...)
        // n and m : frequency of a given #word in a window of 300 seconds that slides each second

        // Swapping each key and its value : #word and its frequency
        JavaPairDStream<Integer, String> swappedHashtagCount = hashtagCount.mapToPair(pair -> new Tuple2<>(pair._2, pair._1));
        // Resulting hashtagCount RDD : ((n,'#word_1'),
        //                               (m,'#word_2'), ...)
        // n and m : frequency of a given #word in a window of 300 seconds that slides each second

        // Sorting through swappedHashtagCount
        // ERROR HERE, WTF HAPPENS
        JavaPairDStream<Integer, String> sortedSwappedHashtagCount = swappedHashtagCount.transformToPair(pairRdd -> pairRdd.sortByKey(false));
        // Resulting hashtagCount RDD : ((n,'#word_1'),
        //                               (m,'#word_2'), ...)
        // n to m : descending or ascending frequency of a given #word in a window of 300 seconds that slides each second

        // Swapping each key and its value : freauency and #word
//        JavaPairDStream<String, Integer> sortedHashtagCount = sortedSwappedHashtagCount.mapToPair(pair -> new Tuple2<String, Integer>(pair._2, pair._1));
        // Resulting hashtagCount RDD : (('#word_1',n),
        //                               ('#word_2',m), ...)


        swappedHashtagCount.foreachRDD(rdd -> rdd.foreach(content -> System.out.println(content)));
        jssc.checkpoint("~/temp");
        jssc.start();
        jssc.awaitTermination();
    }

    private static void twitterSetup() throws IOException {
        String file = "src/main/resources/twitter_access/twitter.txt";

        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = reader.readLine();
        while (line != null) {
            String[] fields = line.split(" ");
            if (fields.length == 2) {
                String twitter_o_auth_str = "twitter4j.oauth." + fields[0];
                System.setProperty(twitter_o_auth_str, fields[1]);
            }
            line = reader.readLine();
        }


        reader.close();

    }
}
