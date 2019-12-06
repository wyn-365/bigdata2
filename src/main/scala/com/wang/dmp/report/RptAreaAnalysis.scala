package com.wang.dmp.report

import com.wang.dmp.beans.ReportAreaAnalysis
import com.wang.dmp.utils.{ConfigHandler, MySQLHandler}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
 * 地域分布的数据分析
 * SparkCore实现方式
 */
object RptAreaAnalysis {
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

    //导入隐士转换
    import sQLContext.implicits._

    val resultDF = rawDataFrame.map(row =>{
      val pname = row.getAs[String]("provincename")
      val cname = row.getAs[String]("cityname")

      val rmode = row.getAs[Int]("requestmode")
      val pmode = row.getAs[Int]("processmode")

      //请求数量
      val adRequest = if(rmode == 1 && pmode == 3) {
          List[Double] (1,1,1)
        }else if(rmode == 1 && pmode >= 2){
          List[Double](1,1,0)
        }else if(rmode == 1 && pmode == 1){
          List[Double](1,0,0)
        }else{
          List[Double](0,0,0)
        }

      val efftv = row.getAs[Int]("iseffective")
      val bill = row.getAs[Int]("isbilling")
      val bid = row.getAs[Int]("isbid")
      val win = row.getAs[Int]("iswin")
      val orderId = row.getAs[Int]("adorderid")

      //广告的净价数量
      val adRTB = if (efftv == 1 && bid == 1  && bill ==1 && orderId != 0){
          List[Double](1,0)
        }else if(efftv == 1 && bill ==1 && win == 1){
          List[Double](0,1)
        }else{
          List[Double](0,0)
        }

      //展示数，点击数
      val adShowClick = if(rmode == 2 && efftv ==1){
        List[Double](1,0)
      }else if(rmode == 3 && efftv ==1){
        List[Double](0,1)
      }else{
        List[Double](0,0)
      }

      //消费与成本
      val price = row.getAs[Double]("winprice")
      val payment = row.getAs[Double]("adpayment")

      val adCost = if (efftv == 1 && win == 1  && bill ==1){
        List[Double](price/1000.0,payment/1000)
      }else{
        List[Double](0,0)
      }


      //返回一个元祖(key,list(原始请求 有效请求 广告请求 参与竞价数 竞价成功数 展示量 点击量 广告成本 广告消费))
      ((pname,cname),adRequest ++ adRTB ++ adShowClick ++ adCost)
    })
      .reduceByKey((list1,list2)=>list1 zip list2 map(tp =>tp._1 + tp._2)) //计算好的结果
      .map(rs => ReportAreaAnalysis(rs._1._1,rs._1._2,rs._2(0),rs._2(1),rs._2(2),rs._2(3),rs._2(4),rs._2(5),rs._2(6),rs._2(7),rs._2(8)))
      .toDF //隐士转换里面的

    //导入mysql
    MySQLHandler.save2db(resultDF,ConfigHandler.areaAnalysisTable)

    //关闭
    sc.stop()


  }
}
