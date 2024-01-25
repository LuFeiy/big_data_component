package com.atguigu.spark.rdd.dependency

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

class dependency01 {
  var conf: SparkConf = null

  var sc: SparkContext = null

  @Before
  def confPrepare: Unit = {
    conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

  }

  @After
  def stopConf: Unit = {
    sc.stop()
  }


  def printRdd[T](rdd: org.apache.spark.rdd.RDD[T]): Unit = {
    val partitionedData = rdd.mapPartitionsWithIndex { (index, partition) =>
      val partitionContent = partition.map(_.toString).mkString(", ") // 将分区数据转换为逗号分隔的字符串
      Iterator(s"Partition $index: $partitionContent") // 将分区号和分区数据合并到一行中
    }

    partitionedData.collect().foreach(println)
  }

  def printRdd[T](rdd: org.apache.spark.rdd.RDD[T], rddInfo: String): Unit = {
    val partitionedData = rdd.mapPartitionsWithIndex { (index, partition) =>
      val partitionContent = partition.map(_.toString).mkString(", ") // 将分区数据转换为逗号分隔的字符串
      Iterator(s"$rddInfo - Partition $index: ($partitionContent)") // 将RDD信息、分区号和分区数据合并到一行中，并加上括号
    }

    partitionedData.collect().foreach(println)
  }


  //查看血缘
  @Test
  def test_01(): Unit = {
    val fileRDD = sc.textFile("input/a.txt")
    println(fileRDD.toDebugString)

    val wordRdd = fileRDD.flatMap(_.split(" "))
    println(wordRdd.toDebugString)

    val mapRdd = wordRdd.map((_, 1))
    println(mapRdd.toDebugString)

    val resRdd = mapRdd.reduceByKey(_ + _)
    println(resRdd.toDebugString)

    resRdd.collect().foreach(println)

  }

  //查看依赖
  @Test
  def test_02(): Unit = {
    val fileRDD = sc.textFile("input/a.txt")
    println(fileRDD.dependencies)

    val wordRdd = fileRDD.flatMap(_.split(" "))
    println(wordRdd.dependencies)

    val mapRdd = wordRdd.map((_, 1))
    println(mapRdd.dependencies)

    val resRdd = mapRdd.reduceByKey(_ + _)
    println(resRdd.dependencies)

    resRdd.collect().foreach(println)

  }
}
