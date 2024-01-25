package com.atguigu.spark.rdd.serializable

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object serializable02 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建一个RDD
    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

    //3.1创建一个Search对象
    val search = new Search("hello")

    // Driver：算子以外的代码都是在Driver端执行

    // Executor：算子里面的代码都是在Executor端执行
    //3.2 函数传递
    search.getMatch1(rdd).collect().foreach(println)
    //3.3 属性传递
    search.getMatch2(rdd).collect().foreach(println)


    //4.关闭连接
    sc.stop()
  }


}


case class Search(query:String) extends Serializable {
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
