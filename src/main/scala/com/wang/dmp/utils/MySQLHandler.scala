package com.wang.dmp.utils

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

object MySQLHandler {


  def save2db(resultDF: DataFrame,tblName:String,partition:Int = 2)={
    resultDF.coalesce(partition).write.mode(SaveMode.Overwrite)jdbc(
      ConfigHandler.url,
      tblName,
      ConfigHandler.dbProps
    )
  }
}
