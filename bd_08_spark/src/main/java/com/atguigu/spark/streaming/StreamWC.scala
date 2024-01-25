package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamWC {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkWC").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(3))

    val socketDS:ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    val flatMapDS = socketDS.flatMap(_.split(" "))

    val mapDS = flatMapDS.map((_, 1))

    val resDS = mapDS.reduceByKey(_ + _)

    resDS.print()

    ssc.start()

    ssc.awaitTermination()
  }

}
