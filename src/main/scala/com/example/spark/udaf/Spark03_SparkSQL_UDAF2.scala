package com.example.spark.udaf

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._


/**
 * Created by 19921224 on 2023/9/26 17:48
 */
object Spark03_SparkSQL_UDAF2 {
    def main(args: Array[String]): Unit = {
        // TODO 创建 SparkSession 运行环境
        // 构造 SparkConf
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
        // 构造 SparkSession
        val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        // 导入 SparkSession 的 隐式转换规则
        //        import sparkSession.implicits._

        val df: DataFrame = sparkSession.read.json("data/user.json")
        df.createOrReplaceTempView("user")
        // 自定义函数 prefixName 的实现，输入是 一个 String 的 参数，返回也是一个 String
        sparkSession.udf.register("ageAvg", functions.udaf(new MyAvgUDAF))
        sparkSession.sql("select ageAvg(age) from user").show()




        // TODO 关闭 SparkSession
        sparkSession.close()
    }

    /**
     * 自定义聚合函数类：计算年龄的平均值 【强类型自定义聚合函数】
     * 1. 继承 org.apache.spark.sql.expressions.Aggregator，定义泛型
     * IN：输入的数据类型 Long
     * BUF：缓冲区的数据类型 Buff (这个我们给一个样例类)
     * OUT：输出的数据类型 Long
     * 2. 重写 方法(6)
     */
    case class Buff(var total: Long, var count: Long)

    class MyAvgUDAF extends Aggregator[Long, Buff, Long] {
        // z & zero 初始值或零值
        override def zero: Buff = {
            Buff(0L, 0L)
        }

        // 根据输入的数据更新缓冲区的数据
        override def reduce(buff: Buff, in: Long): Buff = {
            buff.total = buff.total + in
            buff.count = buff.count + 1
            buff // 有返回值类型是 Buff
        }

        // 合并缓冲区
        override def merge(buff1: Buff, buff2: Buff): Buff = {
            buff1.total = buff1.total + buff2.total
            buff1.count = buff1.count + buff2.count
            buff1
        }

        // 计算结果
        override def finish(buff: Buff): Long = {
            buff.total / buff.count
        }

        // 缓冲区的编码操作 下面这两个方法写法是固定的：如果是自定义的类，则编码就是 product；若是scala已存在的类，则是对应的类
        override def bufferEncoder: Encoder[Buff] = Encoders.product

        // 输出的编码操作
        override def outputEncoder: Encoder[Long] = Encoders.scalaLong
    }
}
