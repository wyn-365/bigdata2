package com.wang.dmp.etl

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}


/**
 * 原始文件装换成parquet格式
 */
object Bz2Parquet {
  def main(args: Array[String]): Unit = {
    //判断输入路径
    if (args.length !=2){
      println("参数不合法")
      sys.exit()
    }
    //模式匹配
    val Array(dataInputPath,outPutPath) = args

    //配置文件
    val sparkConf = new SparkConf()
      .setAppName("日志文件转换成parquet格式")
      .setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")//序列化要求


    //创建一个sparkContext
    val sc = new SparkContext(sparkConf)


    //读取原始数据
    val rawData: RDD[String] = sc.textFile(dataInputPath)

    //处理数据
    val arrRdd: RDD[Array[String]] = rawData
      .map(line => line.split(",",-1))
      //过滤非法数据不符合要求的数据
      .filter(_.length >=85)

    //RDD[Array[String]] => RDD[Row]
    val rowRdd:RDD[Row] = arrRdd.map(arr =>{
      Row(
        arr(0),//sessionid:String
        arr(1).toInt,//advertisered:Int
        arr(2).toInt,//adorderid:Int
        arr(3).toInt,
        arr(4).toInt,
        arr(5),
        arr(6),
        arr(7).toInt,
        arr(8).toInt,
        arr(9).toDouble,
        arr(10).toDouble,
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        arr(17).toInt,
        arr(18),
        arr(19),
        arr(20).toInt,
        arr(21).toInt,
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        arr(26).toInt,
        arr(27),
        arr(28).toInt,
        arr(29),
        arr(30).toInt,
        arr(31).toInt,
        arr(32).toInt,
        arr(33),
        arr(34).toInt,
        arr(35).toInt,
        arr(36).toInt,
        arr(37),
        arr(38).toInt,
        arr(39).toInt,
        arr(40).toDouble,
        arr(41).toDouble,
        arr(42).toInt,
        arr(43),
        arr(44).toDouble,
        arr(45).toDouble,
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        arr(57).toInt,
        arr(58),
        arr(59).toInt,
        arr(60).toInt,
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        arr(73).toInt,
        arr(74).toDouble,
        arr(75).toDouble,
        arr(76).toDouble,
        arr(77).toDouble,
        arr(78).toDouble,
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        arr(84).toInt //mediatype: Int
      )
    })

    //表结构一样
    val schema = StructType(Seq(
        StructField()
    ))

    //判断输出目录是否存在？ 存在的话就删除
    //val file = new File(outPutPath)
    //if(file.exists()){
    //  file.delete()
    //}
    //文件路径判断
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(outPutPath)
    if(fs.exists(path)){
      fs.delete(path,true)
    }

    //保存数据
    val sQLContext = new SQLContext(sc)
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")
    val dataFrame = sQLContext.createDataFrame(rowRdd,schema)

    //输出文件
    dataFrame.write.parquet(outPutPath)
    //关闭 37
    sc.stop()
  }
}
