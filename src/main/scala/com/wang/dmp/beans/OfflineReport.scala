package com.wang.dmp.beans

case class ReportLogDataAnalysis(provinceName:String,cityName:String,ct:Int)

case class ReportAreaAnalysis(provinceName:String,
                              cityName:String,
                              rawReq:Double,
                              effReq:Double,
                              adReq:Double,
                              rtbReq:Double,
                              winReq:Double,
                              adShow:Double,
                              adClick:Double,
                              adCost:Double,
                              adPayment:Double
                             )

case class ReportMediaAnalysis(appName:String,
                              rawReq:Double,
                              effReq:Double,
                              adReq:Double,
                              rtbReq:Double,
                              winReq:Double,
                              adShow:Double,
                              adClick:Double,
                              adCost:Double,
                              adPayment:Double
                             )