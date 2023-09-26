package com.example.spark.sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by 19921224 on 2023/9/26 17:15
 * 任务需求：
 * 已知RDD[(query:String, item_id:String, imp:Int, clk:Int)]，
 * 要求找到每个query对应的点击最多的前2个item_id，即：按照query分组，并按照clk降序排序，每组取前两个。
 *
 * 例如：
 * （连衣裙，1234,  22,  13）
 * （牛仔裤，2768,  34,  7）
 * （连衣裙，1673，45,  9）
 * （衬衣，3468， 67,  12）
 * （牛仔裤，2754, 68， 20）
 * （连衣裙，1976，93,  29）
 *
 * 希望得到：
 * （连衣裙，1976，93,  29）
 * （连衣裙，1234,  22,  13）
 * （牛仔裤，2754, 68， 20）
 * （牛仔裤，2768,  34,  7）
 * （衬衣，3468， 67,  12）
 */
object Spark01_RDD_Sort {
    def main(args: Array[String]): Unit = {
        // TODO 准备环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        //    sparkConf.set("spark.default.parallelism", "3")
        val sc = new SparkContext(sparkConf)

        // TODO 创建 RDD
        val rdd: RDD[String] = sc.textFile("datas/3.txt")

        val sourceRDD: RDD[(String, (String, String, String))] = rdd.map(
            line => {
                val lines: Array[String] = line.split("\\s")
                lines.toList
                (lines(0), (lines(1), lines(2), lines(3)))
            }
        )

        val mapRDD: RDD[(String, List[(String, String, String)])] = sourceRDD.groupByKey().mapValues(
            iter => {
                iter.toList.sortBy(_._1.toInt)
            }
        )

        mapRDD.collect().foreach(println)


        //        newRDD.saveAsTextFile("output1")

        // TODO 关闭环境
        sc.stop()
    }
}
