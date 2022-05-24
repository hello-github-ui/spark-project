package com.example.spark.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created on 2022/5/24 20:55
 */
object WordCount {
    def main(args: Array[String]): Unit = {
        // 创建 Spark 运行配置对象
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        // 创建 Spark 上下文环境对象(连接对象)
        val sc = new SparkContext(sparkConf)
        // 读取文件数据 hdfs:/// 默认是 当前 NameNode 的地址 <=> hdfs://bigdata101/input
        val fileRDD: RDD[String] = sc.textFile("hdfs:///input/word.txt")
        // 将文件中的数据进行分词
        val wordRDD: RDD[String] = fileRDD.flatMap((_: String).split("\\s"))
        // 转换数据结构 word => (word, 1)
        val word2OneRDD: RDD[(String, Int)] = wordRDD.map(((_: String), 1))
        // 将转换结构后的数据按照相同的单词进行分组聚合
        val word2CountRDD: RDD[(String, Int)] = word2OneRDD.reduceByKey((_: Int) + (_: Int))
        // 将数据聚合结果采集到内存中
        val word2Count: Array[(String, Int)] = word2CountRDD.collect()
        // 打印结果
        word2Count.foreach(println)
        // 关闭 Spark 连接
        sc.stop()
    }
}
