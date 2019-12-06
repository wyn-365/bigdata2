package com.wang.dmp.utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

object FileHandler {
    def deleteWillOutputDir(sc:SparkContext,outPutPath:String)={
      //文件路径判断
      val fs = FileSystem.get(sc.hadoopConfiguration)
      val path = new Path(outPutPath)
      if(fs.exists(path)){
        fs.delete(path,true)
      }

    }
}
