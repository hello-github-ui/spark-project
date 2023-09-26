package com.example.spark.udf

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Created by 19921224 on 2023/9/26 16:35
 * 需求：
 * 如果想 给 user.json 中 展示的 username 字段加一个 前缀，就需要我们的 udf （自定义函数了）
 */
object MyUDF {
    def main(args: Array[String]): Unit = {
        // 执行环境
        val spark: SparkSession = SparkSession.builder()
                .appName("Operate Parquet File")
                .master("local[*]")
                .getOrCreate()

        // 读取 user.json
        /// 由于该 json 文件是多行的，故需要加入配置 option("multiline", "true")
        val df: DataFrame = spark.read.option("multiline", "true").json("data/user/user.json")
        //        df.show()

        // 先注册为一个临时视图
        df.createOrReplaceTempView("user")

        // 自定义函数 udf
        /// 实现功能：在某一列前面拼接字符串，输入是字符串，输出也是一个字符串
        spark.udf.register("prefixName", (name: String) => {
            "我是: " + name
        })

        // 查询
        spark.sql("select age, prefixName(username) as username from user").show()

        // 关闭
        spark.close()
    }
}
