package com.atguigu.spark.rdd.serializable

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


object serializable03 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("SerDemo")
      .setMaster("local[*]")
      // 替换默认的序列化机制
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 注册需要使用 kryo 序列化的自定义类
      .registerKryoClasses(Array(classOf[Searcher]))

    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello atguigu", "atguigu", "hahah"), 2)

    val searcher = new Searcher("hello")
    val result: RDD[String] = searcher.getMatch1(rdd)

    result.collect.foreach(println)

  }


}



case class Searcher(query:String) extends Serializable {
  def isMatch(s:String):Boolean = {
    s.contains(query)
  }

  def getMatch1(rdd:RDD[String]):RDD[String] = {
    rdd.filter(isMatch)
  }

  def getMatch2(rdd: RDD[String]): RDD[String] = {
    rdd.filter(x=>x.contains(query))
  }

}



