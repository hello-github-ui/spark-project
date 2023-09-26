package com.example.spark.sort

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by 19921224 on 2023/9/26 17:16
 */
object Spark01_RDD_Sort_Value {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("RDDTest")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(List(
            ("a", 2), ("a", 4), ("a", 3), ("a", 1), ("b", 5), ("b", 8), ("b", 7), ("b", 6)
        ))
        //根据key分组并内部降序
        rdd1.groupByKey().mapValues(
            iter => {
                //分组内部排序的两种方式
                // iter.toList.sorted.reverse
                iter.toList.sortWith(_ < _)
            }).foreach(println)
        sc.stop()
    }
}
