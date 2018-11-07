package com.chenhao.Graphx;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.GraphLoader;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

//计算双向边和重复边 distinct mask reverse
public class Duplicate2Way {

    private static final ClassTag<String> tagString = ClassTag$.MODULE$.apply(String.class);
    private static final ClassTag<Double> tagDouble = ClassTag$.MODULE$.apply(Double.class);
    private static final ClassTag<Integer> tagInteger = ClassTag$.MODULE$.apply(Integer.class);
    private static final ClassTag<Object> tagObject = ClassTag$.MODULE$.apply(Object.class);

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
//        Graph<Integer,Integer> GraphLoader.edgeListFile();
    }
}
