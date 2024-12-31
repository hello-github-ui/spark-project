import java.io.IOException;
import java.nio.file.FileSystem;

import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.asc;

public class NASDAQ_Stock_Trade_Report {

    private static String tableName = "NASDAQ_Stock_Trade_Report";

    public static void main(String[] args) {
        // 获取传入的日期参数
        String date = args[0];
        String month = date.substring(0, 6);

        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.shuffle.io.maxRetries", "60");
        sparkConf.set("spark.shuffle.io.retryWait", "60s");
        // _SUCCESS
        sparkConf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
        // Hadoop 的身份认证和授权使用 Kerberos 方式认证
        sparkConf.set("hadoop.security.authentication", "Kerberos");
        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .appName(tableName)
                .getOrCreate();

        // HDFS FileSystem 对象
        FileSystem fs = null;
        try{
            fs = FileSystem.get(spark.sparkContext()).hadoopConfiguration());
        }catch(IOException e){
            throw new RuntimeException(e.getCause());
        }

        // Kerberos 身份认证
        System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");

        // 读取路径
        String sourcePath = "hdfs://nameservice1/htdata/mdc/MDCProvider/" + tableName + "_Day/tradingday=" + month + "*/";

        Dataset<Row> supplyUnionOrigin = spark.read().format("parquet").load(sourcePath);

        // 临时写入路径
        String writeTemporaryPath = "hdfs://nameservice1/htdata/mdc/MDCProvider/" + tableName + "_Month/month=" + "temp" + "/";

        // 最终写入路径
        String writePath = "hdfs://nameservice1/htdata/mdc/MDCProvider/" + tableName + "_Month/month=" + month + "/";

        // 若当日目录存在，则删除
        if(fs.exists(new Path(writeTemporaryPath))){
            fs.delete(new Path(writeTemporaryPath), true);
        }

        // 按照 HTSCSecurityID 标的 分区，每一个 HTSCSecurityID 写入一个单独的文件，在单文件内，按照 MDDate，MDTime 排序
        supplyUnionOrigin.select(
            "MDDate", // 行情日期YYYYMMDD
            "MDTime", // 行情时间HHMMSSsss
            "ExchangeDate",
            "ExchangeTime",
            "Nanosecond", // 6位纳秒时间
            "SecurityType", // 证券类型，华泰自定义
            "SecuritySubType", // nasdaq原始定义
            "SecurityID", // 原始代码
            "SecurityIDSource", // 证券市场，华泰自定义
            "MDChannel", // 行情接入渠道：NASDAQChannel
            "TradingPhaseCode", // 只收Stock Trading状态更新
            "TradeNum", // 成交编号（Trade Control Number)
            "OriginalTradeNum", // 原始成交编号，用于Trade Correction揭示原始成交编号
            "TradeType", // 成交类别
            "TradePrice", // 成交价格
            "TradeQty", // 成交数量
            "HTSCSecurityID", // 华泰产品代码由原始代码加.和证券市场缩写组成，华泰自定义
            "ReceiveDateTime", // 接收到行情数据的时间YYYYMMDDHHMMSSsss
            "ChannelNo", // 交易所原始频道代码（Originating Market Center Identifier)
            "NAVOffsetAmount", // 用于NextShares计算TradePrice
            "TotalConsolidateVolume", // 全市场总成交量，只有美股有值
            "SaleConditionLV1",
            "SaleConditionLV2",
            "SaleConditionLV3",
            "SaleConditionLV4",
            "TrackingNm" // Nasdaq internal tracking number
        ).repartition(new Column("HTSCSecurityID"))
        .withColumn("partition_HSID", new Column("HTSCSecurityID"))
        .sortWithinPartitions(
            asc("partition_HSID"),
            asc("MDDate"),
            asc("MDTime")
        )
        .write()
        .partitionBy("partition_HSID")
        .parquet(writeTemporaryPath);

        // 修复文件名以及移动HDFS目录
        Path level1Path = new Path(writeTemporaryPath);
        try{
            FileStatus[] fileStatuses = fs.listStatus(level1Path);
            for(FileStatus fileStatus : fileStatuses) {
                if(fileStatus.isDirectory()) {
                    String name = fileStatus.getPath().getName();
                    if(name != null && name.contains("=")) {
                        if("partition_HSID".equalsIgnoreCase(name.split("=")[0])){
                            String securityId = name.split("=")[1];
                            FileStatus[] subFiles = fs.listStatus(fileStatus.getPath());
                            for(FileStatus subFile : subFiles) {
                                if(subFile.isFile()) {
                                    fs.rename(
                                        subFile.getPath(),
                                        new Path(
                                            writeTemporaryPath + tableName + "_" + securityId + "_" + month + ".parquet"
                                        )
                                    );
                                    // 重命名后，将原文件删除
                                    fs.delete(fileStatus.getPath(), false);
                                }else{
                                    throw new Exception(String.format("Path %s has more than 1 files", fileStatus.getPath().toString()));
                                }
                            }
                        }
                    }else{
                        fs.delete(fileStatus.getPath(), true);
                    }
                }else{
                    fs.delete(fileStatus.getPath(), true);
                }
            }
        }catch(Exception e){
            throw new RuntimeException(e.getCause());
        }

        // 删除旧的目录
        if(fs.exists(new Path(writePath))){
            fs.delete(new Path(writePath), true);
        }

        // 重命名
        fs.rename(new Path(writeTemporaryPath), new Path(writePath));

        fs.close();
        spark.close();
    }

}
