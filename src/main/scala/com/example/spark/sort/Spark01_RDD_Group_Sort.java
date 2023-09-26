package com.example.spark.sort;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Created by 19921224 on 2023/9/26 17:13
 */
public class Spark01_RDD_Group_Sort {
    public static void main(String[] args) {
        /**
         * 创建spark配置对象SparkConf，设置spark运行时配置信息，
         * 例如通过setMaster来设置程序要连接的集群的Master的URL，如果设置为local，
         * spark为本地运行
         */
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD_Group_Sort");
        /**
         * 创建JavaSparkContext对象
         * SparkContext是spark所有功能的唯一入口，
         * SparkContext核心作用，初始化spark运行所需要的核心组件，同时还会负责spark程序在master的注册。
         *
         */
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("OFF");

        /**
         * 根据数据来源，通过JavaSparkContext来创建RDD
         */
        JavaRDD<String> rdd = sc.textFile("datas/3.txt");

//        JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
//            public Tuple2<String, Integer> call(String line) throws Exception {
//                String[] split = line.split(" ");
//                return new Tuple2<String, Integer>(split[0], Integer.parseInt(split[1]));
//            }
//        });

        JavaPairRDD<String, List> pairRDD = rdd.mapToPair(new PairFunction<String, String, List>() {
            @Override
            public Tuple2<String, List> call(String line) throws Exception {
                String[] split = line.split("\\s");
                List<Object> list = new ArrayList();
                list.add(split[1]);
                list.add(split[2]);
                list.add(split[3]);
                return new Tuple2<>(split[0], list);
            }
        });

        /**
         * 分组
         */
        JavaPairRDD<String, Iterable<List>> groupRDD = pairRDD.groupByKey();

        /**
         * 对分组结果排序
         */
//        JavaPairRDD<String, Iterable<Integer>> groupsSort = groupRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
//            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> groupData) throws Exception {
//                List<Integer> integers = new ArrayList<Integer>();
//                String name = groupData._1;
//                Iterator<Integer> it = groupData._2.iterator();
//                while (it.hasNext()) {
//                    integers.add(it.next());
//                }
//                integers.sort(new Comparator<Integer>() {
//                    public int compare(Integer o1, Integer o2) {
//                        return o2 - o1;
//                    }
//                });
//                return new Tuple2<String, Iterable<Integer>>(name, integers);
//            }
//        });

        JavaPairRDD<String, List> sortRDD = groupRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<List>>, String, List>() {
            @Override
            public Tuple2<String, List> call(Tuple2<String, Iterable<List>> one) throws Exception {
                List<List<Object>> lists = new ArrayList<>();
                String key = one._1;
                Iterator<List> iterator = one._2.iterator();
                while (iterator.hasNext()) {
                    lists.add(iterator.next());
                }
                lists.sort(new Comparator<List<Object>>() {
                    @Override
                    public int compare(List<Object> o1, List<Object> o2) {
                        return Integer.parseInt(o1.get(0).toString()) - Integer.parseInt(o2.get(0).toString());
                    }
                });

                return new Tuple2<>(key, lists);
            }
        });

        /**
         * 打印
         */
//        groupsSort.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
//            public void call(Tuple2<String, Iterable<Integer>> data) throws Exception {
//                System.out.println(data._1 + "  " + data._2);
//            }
//        });

        sortRDD.foreach(new VoidFunction<Tuple2<String, List>>() {
            @Override
            public void call(Tuple2<String, List> data) throws Exception {
                System.out.println(data._1 + " " + data._2);
            }
        });

        /**
         * 关闭JavaSparkContext
         */
        sc.stop();
    }
}
