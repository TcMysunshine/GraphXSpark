package com.chenhao.Graphx;

import akka.dispatch.Foreach;
import com.sun.corba.se.spi.ior.ObjectKey;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Function1;
import scala.Function2;
import scala.Serializable;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.reflect.ClassManifestFactory;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.BoxedUnit;

import java.beans.Transient;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GraphXTest {
    private static final ClassTag<String> tagString = ClassTag$.MODULE$.apply(String.class);
    private static final ClassTag<Double> tagDouble = ClassTag$.MODULE$.apply(Double.class);
    private static final ClassTag<Integer> tagInteger = ClassTag$.MODULE$.apply(Integer.class);
    private static final ClassTag<Object> tagObject = ClassTag$.MODULE$.apply(Object.class);

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        List<Tuple2<Object, String>> vertices = new ArrayList<>();
        vertices.add(new Tuple2<Object, String>(1l, "James"));
        vertices.add(new Tuple2<Object, String>(2l, "Robert"));
        vertices.add(new Tuple2<Object, String>(3l, "Charlie"));
        vertices.add(new Tuple2<Object, String>(4l, "Roger"));
        vertices.add(new Tuple2<Object, String>(5l, "Tony"));

        List<Edge<String>> edges = new ArrayList<>();
        edges.add(new Edge<String>(1L, 2L, "Friend"));
        edges.add(new Edge<String>(2L, 3L, "Friend"));
        edges.add(new Edge<String>(3L, 4L, "Couple"));
        edges.add(new Edge<String>(4L, 5L, "Friend"));

        JavaRDD<Edge<String>> edgeRDD = sc.parallelize(edges);
        JavaRDD<Tuple2<Object, String>> verticesRDD = sc.parallelize(vertices);
        Graph<String, String> g = Graph.apply(verticesRDD.rdd(), edgeRDD.rdd(), "default vertices",
                StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), tagString, tagString);
//        System.out.println();
        g.edges().foreach(new MyFuction1<Edge<String>, BoxedUnit>() {
            @Override
            public BoxedUnit apply(Edge<String> stringEdge) {
                System.out.println(stringEdge);
                return BoxedUnit.UNIT;
            }
        });
        g.vertices().foreach(new MyFuction1<Tuple2<Object, String>, BoxedUnit>() {
            @Override
            public BoxedUnit apply(Tuple2<Object, String> objectStringTuple2) {
                System.out.println(objectStringTuple2._2);
                return null;
            }
        });
//        g.vertices().filter(new MyFuction1<Tuple2<Object, String>, Object>() {
//        })
        Graph<Integer, String> g1 = g.mapVertices(new MyFuction2<Object, String, Integer>() {
            public Integer apply(Object o, String s) {
                return 0;
            }
        }, tagInteger, null);

        g1.vertices().foreach(new MyFuction1<Tuple2<Object, Integer>, BoxedUnit>() {
            @Override
            public BoxedUnit apply(Tuple2<Object, Integer> objectIntegerTuple2) {
                System.out.println(objectIntegerTuple2._2);
                return null;
            }
        });

    }

    public static abstract class MyFuction1<T1, R> extends AbstractFunction1<T1, R> implements Serializable {
    }

    public static abstract class MyFuction2<T1, T2, R> extends AbstractFunction2<T1, T2, R> implements Serializable {
    }
}
