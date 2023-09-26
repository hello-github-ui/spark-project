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
                // 避免生成 _SUCCESS 文件
                .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
                // 避免生成  .crc 文件
                .config("parquet.enable.summary-metadata", "false")
                .master("local[*]")
                .getOrCreate()

        // spark.sparkContext.setLogLevel("ERROR")

        try {
            saveParquet(spark)
        } finally {
            spark.stop()
        }
    }


    /*
    写入数据到本地的parquet文件中
     */
    def saveParquet(spark: SparkSession): Unit = {
        // 导入 SparkSession 的 隐式转换规则
        import spark.implicits._
        // 模拟数据
        val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 20), (2, "lisi", 30), (3, "wangwu", 19)))
        // 这里的 toDF() 函数需要使用到 隐式转换规则，必须手动导入
        val df: DataFrame = rdd.toDF("id", "username", "age")
        df.show()

        // 将 DataFrame 写入
        // 获取当前项目的路径
        var projectPath: String = new File("").getCanonicalPath
        projectPath = projectPath.replaceAll("\\\\", "/")
        // 输出的文件名路径
        val filePath: String = "datas/user/"
        df.write.parquet("file:///" + projectPath + "/" + filePath)

    }

    /*
    fileStr: parquet文件的相对项目根路径的路径
     */
    def readParquet(spark: SparkSession, filePath: String): Unit = {
        // 获取当前项目的路径
        var projectPath: String = new File("").getCanonicalPath
        projectPath = projectPath.replaceAll("\\\\", "/")
        // println(s"""当前项目路径: ${filePath}""")

        // val df: DataFrame = spark.read.parquet("file:///" + filePath + "/datas/XHKG_Warrant_Snapshot_Level2_20230901.parquet")
        val df: DataFrame = spark.read.parquet("file:///" + projectPath + "/" + filePath)
        df.show(10)
    }
}