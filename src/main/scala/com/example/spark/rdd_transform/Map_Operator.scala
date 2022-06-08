package com.example.spark.rdd_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Created on 2022/6/8 22:55
 * map 算子：将处理的数据 逐条 进行映射转换，这里的转换可以是类型的转换，也可以是值的转换
 */
object Map_Operator {
    def main(args: Array[String]): Unit = {
        /*
        在 Spark 2.0 版本之前，我们与 Spark 交互之前必须先创建 SparkConf 和 SparkContext
        然而在 Spark 2.0 中(及以后)，我们可以通过 SparkSession 来实现同样的功能，而不需要显式地再创建 SparkConf、SparkContext以及 SQLContext了，
        因为这些对象已经封装在 SparkSession 中了，使用生成器的设计模式(builder design pattern)，如果我们没有创建 SparkSession 对象，
        则会实例化出一个新的 SparkSession 对象及其相关的上下文
         */
        val spark: SparkSession = SparkSession.builder().appName("Map_Operator").master("local[*]").getOrCreate()
        val rdd: RDD[Int] = spark.sparkContext.makeRDD(List(1, 4, 7, 2, 8))
        val mapRDD: RDD[Int] = rdd.map(
            _ + 1
        )
        mapRDD.foreach(println)
        spark.stop()
    }
}
