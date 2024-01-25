package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object DStreamCreate_RDDQueue {
  def main(args: Array[String]): Unit = {
    // 创建Spark配置信息对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDDStream")
    // 创建SparkStreamingContext
    val ssc = new StreamingContext(conf, Seconds(3))


    val rddQueue = new mutable.Queue[RDD[Int]]()

    val inputStream = ssc.queueStream(rddQueue, oneAtATime = false)

    val mappedStream = inputStream.map((_, 1))

    val reducedStream = mappedStream.reduceByKey(_ + _)

    reducedStream.print()

    ssc.start()

    for(i <- 1 to 5){
      rddQueue += ssc.sparkContext.makeRDD(1 to 5,10)
      Thread.sleep(2000)
    }


    ssc.awaitTermination()


  }
}
