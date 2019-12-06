package com.wang.dmp.utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object RptKpiTools {

  def offLineKpi(row: Row)={

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

    //原始请求 有效请求 广告请求 参与竞价数 竞价成功数 展示量 点击量 广告成本 广告消费))
    adRequest ++ adRTB ++ adShowClick ++ adCost
  }


  val notEmptyAppName = (appId:String,appName:String) =>{

    if(StringUtils.isEmpty(appName)){
      val jedis = JedisPools.getJedis
      val appName = jedis.hget("appdict",appId)
      jedis.close()
      appName
    }else appName
  }
}
