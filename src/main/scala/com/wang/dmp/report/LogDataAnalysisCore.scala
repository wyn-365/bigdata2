package com.wang.dmp.report

import com.wang.dmp.beans.ReportLogDataAnalysis
import com.wang.dmp.utils.{ConfigHandler, MySQLHandler}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
 * * 统计日志文件中各省份数据分布情况
 * * Core  RDD
 */
object LogDataAnalysisCore {
  def main(args: Array[String]):Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("统计日志文件中各省份数据分布情况")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //序列化

    //创建spark  sql
    val sparkContext = new SparkContext(sparkConf)
    val sQLContext = new SQLContext(sparkContext)

    //读取数据 配置文件中的路径
    val rawDataFrame = sQLContext.read.parquet(ConfigHandler.parquetPath)

    //（key,value） 统计结果
    val result= rawDataFrame.map(row =>{
      val pname = row.getAs[String]("provincename")
      val cname = row.getAs[String]("cityname")
      ((pname,cname),1)
    }).reduceByKey(_+_)

    //ReportLogDataAnalysis 一个实体类  对象换换成json
    import sQLContext.implicits._
    val resultDF = result.map(tp => ReportLogDataAnalysis(tp._1._1,tp._1._2,tp._2)).toDF()

    //把结果写成转换成json格式 [合并分区1个]
    //resultDF.coalesce(1).write.json(ConfigHandler.logdataAnalysisResultJsonPath)

    //把结果写出到mysql中 库中存在表的话就删除
    //resultDF.write.mode(SaveMode.Overwrite).jdbc(ConfigHandler.url,ConfigHandler.table,ConfigHandler.dbProps)
    MySQLHandler.save2db(resultDF,ConfigHandler.table)

    //关闭
    sparkContext.stop()
  }
}
