package com.example.spark

import org.apache.spark.api.java.JavaSparkContext.fromSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created on 2022/5/24 20:58
 * 使用 flatMap + reduceByKey 实现 wordCount 功能
 */
object HelloWorld {
    def main(args: Array[String]): Unit = {
        // 1. 创建 SparkConf // 如果是 Yarn 集群部署模式，就不能设置 setMaster() 属性了
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HelloWorld")
        // 2. SparkContext 上下文环境
        val sc: SparkContext = new SparkContext(sparkConf)
        // 3. 业务操作
        val rdd: RDD[String] = sc.textFile("input/word.txt")
        val flatRDD: RDD[String] = rdd.flatMap(
            line => {
                line.split("\\s")
            }
        )
        val mapRDD: RDD[(String, Int)] = flatRDD.map(
            word => {
                (word, 1)
            }
        )
        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
        reduceRDD.foreach(println)
        // 4. 关闭 SparkContext
        sc.close()
    }
}
