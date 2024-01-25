package com.atguigu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountLocal {

  def main(args: Array[String]): Unit = {

    //1. 创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")

    //2. 创建SparkContext对象，该对象是提交spark app的入口
    val sparkContext = new SparkContext(conf)

    //3. 指定读取文件
    val lineRdd:RDD[String] = sparkContext.textFile("D:\\DataScience\\Code\\big_data_component\\bd_08_spark\\input")

    //4. 扁平化处理一行数据
    val wordRdd:RDD[String] = lineRdd.flatMap(line => line.split(" "))

    //5. 转换数据结构
    val wordToMapRdd:RDD[(String, Int)] = wordRdd.map(word => (word, 1))

    //6. 聚合处理
    val wordToSumRdd:RDD[(String,Int)] = wordToMapRdd.reduceByKey((v1, v2) => v1 + v2)

    //7. 统计结果采集
    val wordToCountArray: Array[(String, Int)] = wordToSumRdd.collect()

    //8.输出到控制台
    //wordToSumRdd.saveAsTextFile(args(1))
    wordToCountArray.foreach(println)

    //9. 关闭连接
    sparkContext.stop()

  }

}
