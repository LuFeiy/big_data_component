package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit.Test

class transformOperator {


  @Test
  def test_01():Unit = {
    //创建SparkConf
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(3))
    //创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    //转换为RDD操作
    val wordAndCountDStream: DStream[(String, Int)] = lineDStream.transform(rdd => {
      val words: RDD[String] = rdd.flatMap(_.split(" "))
      val wordAndOne: RDD[(String, Int)] = words.map((_, 1))
      val value: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
      value
    })
    //打印
    wordAndCountDStream.print
    //启动
    ssc.start()
    ssc.awaitTermination()
  }


  @Test
  def test_02(): Unit = {
    //创建SparkConf
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(3))
    //设置检查点路径  用于保存状态
    ssc.checkpoint("D:\\DataScience\\Code\\big_data_component\\bd_08_spark\\cp")
    //创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    //扁平映射
    val flatMapDS: DStream[String] = lineDStream.flatMap(_.split(" "))
    //结构转换
    val mapDS: DStream[(String, Int)] = flatMapDS.map((_, 1))


    val stateDS:DStream[(String, Int)] = mapDS.updateStateByKey(
      (seq: Seq[Int], state: Option[Int]) => {
        Option(seq.sum + state.getOrElse(0))
      }
    )

    //打印输出
    stateDS.print()
    //启动
    ssc.start()
    ssc.awaitTermination()

  }


  @Test
  def test_03(): Unit = {
    //创建SparkConf
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(3))
    //设置检查点路径  用于保存状态
    ssc.checkpoint("D:\\DataScience\\Code\\big_data_component\\bd_08_spark\\cp")
    //创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    //扁平映射
    val flatMapDS: DStream[String] = lineDStream.flatMap(_.split(" "))

    val windowDS: DStream[String] = flatMapDS.window(Seconds(6), Seconds(3))

    //结构转换
    val mapDS: DStream[(String, Int)] = windowDS.map((_, 1))


    val stateDS: DStream[(String, Int)] = mapDS.updateStateByKey(
      (seq: Seq[Int], state: Option[Int]) => {
        Option(seq.sum + state.getOrElse(0))
      }
    )

    //打印输出
    stateDS.print()
    //启动
    ssc.start()
    ssc.awaitTermination()

  }

}
