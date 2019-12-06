package com.wang.dmp.report

import com.wang.dmp.utils.{ConfigHandler, MySQLHandler, RptKpiTools}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
 * sql版本
 */
object RptMdeiaAnalysisRedis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("媒体分析")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //序列化

    //创建spark  sql
    val sc = new SparkContext(sparkConf)
    val sQLContext = new SQLContext(sc)


    //读取数据 配置文件中的路径 非空的数据
    val rawDataFrame = sQLContext.read.parquet(ConfigHandler.parquetPath)
      .filter("appid!=null or appid!='' or appname!=null or appname!=''")

    //创建虚拟视图
    rawDataFrame.registerTempTable("log")

    //用户自定义一个函数
    sQLContext.udf.register("NotEmptyAppName",RptKpiTools.notEmptyAppName)

    //sql
    val result = sQLContext.sql(
      """
        |select NotEmptyAppName(appid,appname) appName,
        |sum(case when requestmode =1 and processnode>=1 then 1 else 0 end ) rawReq,
        |sum(case when requestmode =1 and processnode>=2 then 1 else 0 end ) effReq,
        |sum(case when requestmode =1 and processnode =3 then 1 else 0 end ) adReq,
        |
        |sum(case when iseffective=1 and isbilling=1 and isbid=1 and adorderid!=0 then 1 else 0 end ) rtbReq,
        |sum(case when iseffective=1 and isbilling=1 and iswin =1 then 1 else 0 end ) winReq,
        |
        |sum(case when requestmode =2 and iseffective =1 then 1 else 0 end ) adShow,
        |sum(case when requestmode =3 and iseffective =1 then 1 else 0 end ) adClick,
        |
        |sum(case when iseffective=1 and isbilling=1 and iswin =1 then winprice/1000 else 0 end ) adCost,
        |sum(case when iseffective=1 and isbilling=1 and iswin =1 then adpayment/1000 else 0 end ) adPayment,
        |from log group by NotEmptyAppName(appid,appname)
        |""".stripMargin)


    //控制台查看
    //result.show()

    //存入MySQL数据库 覆盖数有的话 封装好了
    MySQLHandler.save2db(result,ConfigHandler.mediaAnalysisTable)

    //关闭
    sc.stop()
  }
}
