package com.wang.dmp.report

import com.wang.dmp.utils.{ConfigHandler, FileHandler}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
 * 统计日志文件中各省份数据分布情况
 * SparkSQL
 */
object LogDataAnalysis {
      def main(args: Array[String]):Unit = {
        val sparkConf = new SparkConf()
        sparkConf.setMaster("local[*]")
        sparkConf.setAppName("统计日志文件中各省份数据分布情况")
        sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")//序列化

        //创建spark  sql
        val sparkContext = new SparkContext(sparkConf)
        val sQLContext = new SQLContext(sparkContext)

        //读取数据 配置文件中的路径
        val rawDataFrame = sQLContext.read.parquet(ConfigHandler.parquetPath)

        //注册临时表
        rawDataFrame.registerTempTable("log")

        //本剧省份和地市查询这张表
        val result = sQLContext.sql(
          """
            |select count(*) ct,provincename,cityname
            |from log group by provincename,cityname
            """.stripMargin
        )

        //若输出文件存在则删除
        FileHandler.deleteWillOutputDir(sparkContext,ConfigHandler.logdataAnalysisResultJsonPath)

        //把结果写成转换成json格式 [合并分区1个]
        result.coalesce(1).write.json(ConfigHandler.logdataAnalysisResultJsonPath)

        //把结果写出到mysql中 库中存在表的话就删除
        result.write.mode(SaveMode.Overwrite).jdbc(ConfigHandler.url,ConfigHandler.table,ConfigHandler.dbProps)

        //关闭
        sparkContext.stop()
      }
}
