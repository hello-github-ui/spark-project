package com.example.spark.parquet

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import java.io.File

/**
 * Created by 19921224 on 2023/9/25 18:08
 * 读取本地的parquet文件
 */
object OperateFile {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
                .appName("Operate Parquet File")
                .master("local[*]")
                .getOrCreate()

        // 不用这种方式，采用直接修改 log4j2.properties 方式
        // spark.sparkContext.setLogLevel("ERROR")

        // 避免生成  parquet.crc 文件
        spark.conf.set("spark.hadoop.parquet.enable.summary-metadata", "false")
        // 避免生成 _SUCCESS 文件
        spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

        // 获取当前项目的路径
        var projectPath: String = new File("").getCanonicalPath
        projectPath = projectPath.replaceAll("\\\\", "/")

        try {
            /** ** 读取 json 文件，并输出为 df ** */
            // 读取 json 文件
            val filePath: String = "data/user/user_info.json"
            val df: DataFrame = readJSON(spark, filePath)
            // df.show()
            df.select("id", "name", "age", "sex", "phone", "address", "birthday", "hobby").show()

            /** ** 将该 df 写入 parquet ** */
            // 写入 data/parquet/output/ 目录下
            val parquetOutputDir: String = "file:///" + projectPath + "/" + "data/parquet/output/"
            writeParquet(df, parquetOutputDir)

            // 删除掉生成的 parquet.crc 文件，不知道为啥上面已经设置了不生成该文件结果还是生成了
            // val parquetDir: File = new File(projectPath + "/" + "data/parquet/output/")
            // val files: Array[File] = parquetDir.listFiles()
            // for (file <- files) {
            //     if (file.isFile){
            //         if ("crc".equals(file.getName.split("\\.")(1))){
            //             file.delete()
            //         }else{
            //             file.renameTo(new File(projectPath + "/" + "data/parquet/output/" + "user_info.parquet"))
            //         }
            //     }
            // }
            // println("重命名parquet完成")

            /** ** 再从写入的parquet文件中读取parquet，此处需要传入目录 ** */
            val parquetDF: DataFrame = readParquet(spark, parquetOutputDir)
            parquetDF.select("id", "name", "age").show(10)


        } finally {
            spark.stop()
        }
    }


    /**
     * 将 Dataset 数据集写入到parquet文件中
     */
    private def writeParquet(spark: SparkSession, df: DataFrame, output: String): Unit = {
        // 导入隐式转换规则，用于 Dataframe <=> Dataset
        import spark.implicits._
        // 使用样例类（UserInfo）将 DataFrame 转换为 Dataset
        val ds: Dataset[UserInfo] = df.as[UserInfo]
        ds.repartition(1).write.parquet(output)
        println("将 ds 写入 parquet 文件完成 ...")
    }

    /**
     * 将 DataFrame 数据集写入 parquet 文件中
     */
    private def writeParquet(df: DataFrame, output: String): Unit = {
        // 使用样例类（UserInfo）将 DataFrame 转换为 Dataset
        df.repartition(1).write.mode(SaveMode.Overwrite).parquet(output)
        println("将 df 写入 parquet 文件完成 ...")
    }


    /**
     * 从内存中构造数据并写入parquet文件中
     */
    def saveParquetFromMemory(spark: SparkSession): Unit = {
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
        val filePath: String = "data/parquet/output"
        df.repartition(1).write.parquet("file:///" + projectPath + "/" + filePath)
    }

    /*
    fileStr: parquet文件的相对项目根路径的路径
     */

    /**
     * parquetDir: parquet文件所在的目录(相对项目根路径的路径)
     */
    private def readParquet(spark: SparkSession, parquetDir: String): DataFrame = {
        // 获取当前项目的路径
        // var projectPath: String = new File("").getCanonicalPath
        // projectPath = projectPath.replaceAll("\\\\", "/")
        // println(s"""当前项目路径: ${filePath}""")

        // val df: DataFrame = spark.read.parquet("file:///" + filePath + "/datas/XHKG_Warrant_Snapshot_Level2_20230901.parquet")
        val df: DataFrame = spark.read.parquet(parquetDir)
        df
    }

    /**
     * 读取 json 文件，并返回 DataFrame
     */
    private def readJSON(spark: SparkSession, filePath: String): DataFrame = {
        // 获取当前项目的路径
        var projectPath: String = new File("").getCanonicalPath
        projectPath = projectPath.replaceAll("\\\\", "/")

        // 完整的 json 文件路径
        val jsonFile: String = "file:///" + projectPath + "/" + filePath

        // 读取 json 文件，Spark while processing json data considers each new line as a complete json
        // 因此如果你的 json 文件不在一行时，需要加入如下配置即可解决
        val df: DataFrame = spark.read.option("multiline", "true").json(jsonFile)
        df
    }

    /**
     * 读取 parquet 文件，并返回 DataFrame
     */


    /**
     * user_info 样例类，用于 DataFrame <=> DataSet 类型转换 | DataFrame 加上 格式类型 就是 DataSet ; DataSet 去掉格式类型，就是 DataFrame
     */
    private case class UserInfo(id: BigInt, name: String, age: BigInt, sex: String, phone: String, address: String, birthday: String, hobby: String)
}