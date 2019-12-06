package com.wang.dmp.report

import com.wang.dmp.utils.{ConfigHandler, MySQLHandler}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
 * 地域分布的实现
 * spark sql
 */
object RtpAreaAnalysisSQL {
  def main(args: Array[String]):Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("统计地域分布情况")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //序列化

    //创建spark  sql
    val sc = new SparkContext(sparkConf)
    val sQLContext = new SQLContext(sc)

    //读取数据 配置文件中的路径
    val rawDataFrame = sQLContext.read.parquet(ConfigHandler.parquetPath)

    //注册临时表
    rawDataFrame.registerTempTable("log")

    //吧日志中的原始请求的日志查出来
    val result = sQLContext.sql(
      """
        |select provincename,cityname
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
        |from log group by provincename,cityname
      """.stripMargin)


    //存入数据库到mysql中
    MySQLHandler.save2db(result,ConfigHandler.areaAnalysisTable)


    //关闭
    sc.stop()
  }
}
