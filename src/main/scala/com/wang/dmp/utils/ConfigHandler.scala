package com.wang.dmp.utils

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}

object ConfigHandler {

    private lazy val config: Config = ConfigFactory.load()

    //parquet文件坐在的路径
    val parquetPath = config.getString("parquet.path")

    //json路径
    val logdataAnalysisResultJsonPath = config.getString("rpt.logdataAnalysis")

    //mysql解析配置文件
    val driver: String =config.getString("db.driver")
    val url: String = config.getString("db.url")
    val user: String = config.getString("db.user")
    val password: String = config.getString("db.password")
    val table: String = config.getString("db.logdataAnalysis.table")
    val areaAnalysisTable: String = config.getString("db.areaAnalysis.table")
    val mediaAnalysisTable: String = config.getString("db.mediaAnalysis.table")


    //封装mysql的属性
    val dbProps = new  Properties()
    dbProps.setProperty("driver",driver)
    dbProps.setProperty("user",user)
    dbProps.setProperty("password",password)


    val appDictPath: String = config.getString("appdict")

    val redisHost = config.getString("redis.host")
    val redisPort = config.getInt("redis.port")
    val redisIndex = config.getInt("redis.index")

}
