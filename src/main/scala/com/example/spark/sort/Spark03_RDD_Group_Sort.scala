package com.example.spark.sort


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by 19921224 on 2023/9/26 17:14
 */
object Spark03_RDD_Group_Sort {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark03_RDD_Group_Sort")
        val sc = new SparkContext(sparkConf)

        //连衣裙 1234 22 13
        //牛仔裤 2768 34 7
        //连衣裙 167 345 9
        //衬衣 3468 67 12
        //牛仔裤 2754 68 20
        //连衣裙 197 693 29
        val rdd: RDD[String] = sc.textFile("datas/3.txt")

        // (连衣裙,(1234,22,13))
        // (牛仔裤,(2768,34,7))
        // (连衣裙,(167,345,9))
        val mapRDD: RDD[(String, (Int, Int, Int))] = rdd.map(
            line => {
                val lines: Array[String] = line.split("\\s")
                (lines(0), (lines(1).toInt, lines(2).toInt, lines(3).toInt))
            }
        )

        // (连衣裙, [[1234,22,13], [167,345,9], [197,693,9]])
        val groupByKeyRDD: RDD[(String, Iterable[(Int, Int, Int)])] = mapRDD.groupByKey()

        // 排序：(连衣裙, [[1234,22,13], [197,693,9], [167,345,9]])
        val sortRDD: RDD[(String, List[(Int, Int, Int)])] = groupByKeyRDD.mapValues(
            valueList => {
                valueList.toList.sortWith(_._1 > _._1)
            }
        )


        sortRDD.collect().foreach(println)

        // (连衣裙, [[1234,22,13], [197,693,9], [167,345,9]])
        // (牛仔裤,[[2768,34,7]])


        sc.stop()
    }
}
