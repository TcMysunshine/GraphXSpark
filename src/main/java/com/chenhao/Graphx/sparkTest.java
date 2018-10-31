package com.chenhao.Graphx;
import groovy.lang.Tuple;
import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.codehaus.janino.Java;
import scala.Int;
import scala.Serializable;
import scala.Tuple2;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import com.chenhao.Graphx.TV;
import static com.sun.tools.doclint.Entity.lambda;

public class sparkTest {
    public static class TVComparator implements Comparator<TV>, Serializable {
        public int compare(TV tv1, TV tv2){
            return tv1.getPlay()-tv2.getPlay();
        }
    }
    public static void main(String[] args) {
//        wordCount();
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.textFile("tv.txt");
//        lines.foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });
        JavaPairRDD<String, TV> tv = lines.mapToPair(new PairFunction<String, String, TV>() {
            @Override
            public Tuple2<String, TV> call(String s) throws Exception {
                String[] l = s.split(",");
                return new Tuple2<String, TV>(l[0], new TV(l[0], Integer.valueOf(l[1]), Integer.valueOf(l[2])));
            }
        });
        tv.foreach(new VoidFunction<Tuple2<String, TV>>() {
            @Override
            public void call(Tuple2<String, TV> stringTVTuple2) throws Exception {
                String tv = stringTVTuple2._1;
                TV tempTv = stringTVTuple2._2;
                System.out.println(tv + ":" +String.valueOf(tempTv.getPlay() + ":" + String.valueOf(tempTv.getDm())));
            }
        });
        //按照key值进行累加
        JavaPairRDD<String, TV> tvTotal = tv.reduceByKey(new Function2<TV, TV, TV>() {
            @Override
            public TV call(TV tv, TV tv2) throws Exception {
               return new TV(tv.getTv(), tv.getPlay()+tv2.getPlay(), + tv.getDm() + tv2.getDm());
            }
        });
//        key value 变为 value key 便于排序
        JavaPairRDD<TV, String> tv_TV = tvTotal.mapToPair(new PairFunction<Tuple2<String, TV>, TV, String>() {
            @Override
            public Tuple2<TV, String> call(Tuple2<String, TV> stringTVTuple2) throws Exception {
                return new Tuple2<>(stringTVTuple2._2,stringTVTuple2._1);
            }
        });
        //打印value key的rdd
//        System.out.println("打印value key值");
        tv_TV.foreach(new VoidFunction<Tuple2<TV, String>>() {
            @Override
            public void call(Tuple2<TV, String> tvStringTuple2) throws Exception {
                TV tv = tvStringTuple2._1;
                String tvname = tvStringTuple2._2;
                System.out.println("Value Key --->  " + tvname + ":" + String.valueOf(tv.getPlay()) + ":" + String.valueOf(tv.getDm()));
            }
        });
//        按照值排序后的value-key RDD
        JavaPairRDD<TV, String> sortedtv_TV = tv_TV.sortByKey(new TVComparator());
        sortedtv_TV.foreach(new VoidFunction<Tuple2<TV, String>>() {
            @Override
            public void call(Tuple2<TV, String> tvStringTuple2) throws Exception {
                TV tv = tvStringTuple2._1;
                String tvname = tvStringTuple2._2;
                System.out.println("按照播放量排序" + tvname + ":" + String.valueOf(tv.getPlay()) + ":" + String.valueOf(tv.getDm()));
            }
        });

        tvTotal.foreach(new VoidFunction<Tuple2<String, TV>>() {
            @Override
            public void call(Tuple2<String, TV> stringTVTuple2) throws Exception {
                String tv = stringTVTuple2._1;
                TV tempTv = stringTVTuple2._2;
                System.out.println(tv + ":" + String.valueOf(tempTv.getPlay() + ":" +  String.valueOf(tempTv.getDm())));
            }
        });

//        System.out.println(lines.collect());
//        List<String> wordsList = new ArrayList<>();
//        wordsList.add("Mary Jack lucy");
//        wordsList.add("chen wang zhang");
//        JavaRDD<String> wordsStr = sc.parallelize(wordsList);
//        System.out.println(wordsStr.collect());
//        JavaRDD<String> words = wordsStr.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterable<String> call(String s) throws Exception {
//                return Arrays.asList(s.split(" "));
//            }
//        });
//        System.out.println(words.collect());
//        JavaPairRDD<String, Person> wordCount = words.mapToPair(new PairFunction<String, String, Person>() {
//            @Override
//            public Tuple2<String, Person> call(String s) throws Exception {
//                Person p =new Person(s,12);
//                return new Tuple2<>(s,p);
//            }
//        });
//        wordCount.foreach(new VoidFunction<Tuple2<String, Person>>() {
//            @Override
//            public void call(Tuple2<String, Person> stringPersonTuple2) throws Exception {
//                String user = stringPersonTuple2._1;
//                Person p = stringPersonTuple2._2;
//                System.out.println(user + p.getUsername() + Integer.valueOf(p.getAge()));
//            }
//        });
    }

    public static void wordCount() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<String> wordsList = new ArrayList<>();
        wordsList.add("Hello world Hello");
        wordsList.add("Hello world world");
        JavaRDD<String> wordsStr = sc.parallelize(wordsList);
        System.out.println(wordsStr.collect());
        JavaRDD<String> words = wordsStr.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        System.out.println(words.collect());
        JavaPairRDD<String, Integer> wordCount = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        System.out.println(wordCount.collect());
        wordCount = wordCount.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        System.out.println(wordCount.collect());
    }
}
