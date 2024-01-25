package com.atguigu.spark.accbroadcast

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Acc01 {

  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2. Application：初始化一个SparkContext即生成一个Application；
    val sc: SparkContext = new SparkContext(conf)

    //3. 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    val sumAcc = sc.longAccumulator("sum")

    rdd.foreach(
      num => {
        sumAcc.add(num)
      }
    )

    println(sumAcc)

    sc.stop()

  }

}
