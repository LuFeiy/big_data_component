package com.atguigu.spark.accbroadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object broadcast01 {


  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2. Application：初始化一个SparkContext即生成一个Application；
    val sc: SparkContext = new SparkContext(conf)

    //val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    //val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))
    //rdd1.join(rdd2).collect().foreach(println)

    //模拟上面的操作
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val list: List[(String, Int)] = List(("a", 4), ("b", 5), ("c", 6))

    val broadcastList: Broadcast[List[(String, Int)]] = sc.broadcast(list)

    val resRdd = rdd1.map{
      case (k1,v1) => {
        var v2:Int = 0

        for ((k3, v3) <- broadcastList.value){
          if(k1 == k3) {
            v2 = v3
          }
        }



        (k1,(v1,v2))
      }
    }

    resRdd.foreach(println)
  }
}
