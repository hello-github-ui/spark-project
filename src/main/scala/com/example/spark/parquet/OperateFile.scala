package com.example.spark.parquet
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File

/**
 * Created by 19921224 on 2023/9/25 18:08
 * 读取本地的parquet文件
 */
object OperateFile {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
                .appName("Operate Parquet File")
                .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
                .master("local[*]")
                .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        try {
            // 获取当前项目的路径
            //            var filePath: String = new File("").getCanonicalPath
            //            filePath = filePath.replaceAll("\\\\", "/")
            //            // println(s"""当前项目路径: ${filePath}""")
            //
            //            val df: DataFrame = spark.read.parquet("file:///" + filePath + "/datas/XHKG_Warrant_Snapshot_Level2_20230901.parquet")
            //            df.show(10)

            saveParquet(spark)

        } finally {
            spark.stop()
        }
    }


    /*
    写入数据到本地的parquet文件中
     */
    def saveParquet(spark: SparkSession): Unit = {
        // 模拟数据
        val seq: Seq[(String, Any)] = Seq(("name", "zhangsan"), ("age", 21), ("address", "北京市昌平区大前门楼八字胡同2#13"))
        val rdd: RDD[(String, Any)] = spark.sparkContext.parallelize(seq)
        rdd.saveAsTextFile("datas/1.parquet")
    }
}