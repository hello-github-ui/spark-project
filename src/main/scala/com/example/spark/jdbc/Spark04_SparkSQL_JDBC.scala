package com.example.spark.jdbc

import org.apache.spark.SparkConf
import org.apache.spark.sql._

/**
 * Created by 19921224 on 2023/9/26 17:50
 */
object Spark04_SparkSQL_JDBC {

    def main(args: Array[String]): Unit = {
        // TODO 创建 SparkSession 运行环境
        // 构造 SparkConf
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
        // 构造 SparkSession
        val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        // 导入 SparkSession 的 隐式转换规则

        // 读取 mysql 数据
        val df: DataFrame = sparkSession.read
                .format("jdbc")
                .option("url", "jdbc:mysql://127.0.0.1:3306/shequ-user")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("user", "root")
                .option("password", "123456")
                .option("dbtable", "user")
                .load()

        df.show

        // 保存数据
                df.write
                        .format("jdbc")
                        .option("url", "jdbc:mysql://127.0.0.1:3306/spark_project")
                        .option("driver", "com.mysql.cj.jdbc.Driver")
                        .option("user", "root")
                        .option("password", "123456")
                        .option("dbtable", "user_spark")
                        .save()

        // TODO 关闭 SparkSession
        sparkSession.close()
    }
}
