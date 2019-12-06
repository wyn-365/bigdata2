package com.wang.dmp.report

import com.wang.dmp.beans.ReportMediaAnalysis
import com.wang.dmp.utils.{ConfigHandler, MySQLHandler, RptKpiTools}
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext


/**
 * 媒体分析 基于广播变量 Broadcast
 * spark Core
 */
object RptMediaAnalysis {
  def main(args: Array[String]):Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("媒体分析")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //序列化

    //创建spark  sql
    val sc = new SparkContext(sparkConf)
    val sQLContext = new SQLContext(sc)

    //字典数据广播出去
    val appdictMap = sc.textFile(ConfigHandler.appDictPath)
      .map(line => line.split("\t",-1))
      .filter(_.length >=5)
      .map(arr => (arr(4),arr(1)))
      .collect().toMap //搜集到driver端才能广播出去 map查询效率高，广播一次就好
    //广播
    val appdictBT: Broadcast[Map[String,String]] = sc.broadcast(appdictMap)

    //读取数据 配置文件中的路径
    val rawDataFrame = sQLContext.read.parquet(ConfigHandler.parquetPath)
    import sQLContext.implicits._

    //原生数据处理
    val result = rawDataFrame
      .filter("appid!=null or appid!='' or appname!=null or appname!=''")
      .map(row =>{
      val appId = row.getAs[String]("appid")
      var appName = row.getAs[String]("appname")

      if(StringUtils.isEmpty(appName)){
          appName = appdictBT.value.getOrElse(appId,appId) //找到了，或者没找到
      }
      (appName,RptKpiTools.offLineKpi(row))
    }).reduceByKey{
       (list1,list2) =>list1 zip list2 map(tp => tp._1 + tp._2)
    } .map(rs => ReportMediaAnalysis(rs._1,rs._2(0),rs._2(1),rs._2(2),rs._2(3),rs._2(4),rs._2(5),rs._2(6),rs._2(7),rs._2(8)))
      .toDF //隐士转换里面的


    //存储到数据库
    MySQLHandler.save2db(result,ConfigHandler.mediaAnalysisTable)


    //关闭
    sc.stop()
  }
}
