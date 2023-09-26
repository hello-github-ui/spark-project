package com.example.spark.udaf

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Created by 19921224 on 2023/9/26 16:48
 * UDAF: 自定义聚合函数
 * 需求：
 * 我想对 user.json 中的 age 字段输出 平均值
 */
object MyUDAF {
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

        // 输出 年龄的平均值
        df.createOrReplaceTempView("user")
        spark.udf.register("ageAvg", new MyAvgUDAF)
        spark.sql("select ageAvg(age) from user").show()

        // 关闭
        spark.close()
    }

    /**
     * 自定义 udaf 聚合函数类：计算年龄的平均值
     * 1. 继承 UserDefinedAggregateFunction
     * 2. 重写方法（8）
     */
    class MyAvgUDAF extends UserDefinedAggregateFunction {
        // 输入的数据结构类型
        override def inputSchema: StructType = {
            StructType(
                Array(
                    StructField("age", LongType)
                )
            )
        }

        // 缓冲区数据的结构
        override def bufferSchema: StructType = {
            StructType(
                Array(
                    StructField("total", LongType), // age 的总和
                    StructField("count", LongType) // age 的条数
                )
            )
        }

        // 函数计算结果的数据类型：Out，当函数只有一行代码时，可以省略函数体括号{}
        override def dataType: DataType = {
            LongType
        }

        // 函数的稳定性
        override def deterministic: Boolean = {
            true
        }

        // 缓冲区初始化
        override def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer(0) = 0L // 在 Scala 中，一旦集合中有 update 功能时，上下两种写法是一样的
            buffer(1) = 0L

            // 上面的等价于 下面的写法
            // buffer.update(0, 0L)    // 0表示上面 缓冲区数据的结构 中的第一个结构体的属性，1 表示第二个属性
            // buffer.update(1, 0L)
        }

        // 根据输入的值更新缓冲区数据
        override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
            // buffer.getLong(0) 表示从缓冲区中取出 total;
            // input.getLong(0)表示从 输入数据的结构 中取出第一个元素 age
            buffer.update(0, buffer.getLong(0) + input.getLong(0))

            buffer.update(1, buffer.getLong(1) + 1)
        }

        // 缓冲区数据合并：分布式环境下会有多个缓冲区的数据
        override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
            buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
            buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
        }

        // 计算平均值
        override def evaluate(buffer: Row): Any = {
            buffer.getLong(0) / buffer.getLong(1)
        }
    }
}
