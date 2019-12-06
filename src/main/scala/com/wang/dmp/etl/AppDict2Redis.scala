package com.wang.dmp.etl

import com.wang.dmp.utils.{ConfigHandler, JedisPools}
import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext


/**
 * 把字典中的数据灌入到redis中
 * 连线的数据写入
 */
object AppDict2Redis {
  def main(args: Array[String]):Unit = {
    val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("媒体分析")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //序列化

    //创建spark  sql
    val sc = new SparkContext(sparkConf)
    val sQLContext = new SQLContext(sc)

    //读取文件
    val value = sc.textFile(ConfigHandler.appDictPath)
        .map(line =>line.split("\t",-1))
        .filter(_.length>=5)
        .map(fields => (fields(4),fields(1)))
        .filter(tp => StringUtils.isNotEmpty(tp._1) && StringUtils.isNotEmpty(tp._2))
        .foreachPartition(iter =>{
            var jedis = JedisPools.getJedis //拿到连接池的连接
            iter.foreach(tp =>{
              jedis.hset("appdict",tp._1,tp._2)
            })
          jedis.close()
        })

    //关闭
    sc.stop()

  }
}
