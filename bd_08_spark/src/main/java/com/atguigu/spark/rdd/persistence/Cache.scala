package com.atguigu.spark.rdd.persistence

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

class Cache {

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


  @Test
  def test_01(): Unit = {
    val rdd = sc.textFile("input")

    val flatRdd = rdd.flatMap(_.split(" "))

    val mapRdd = flatRdd.map {
      word => {
        println("******")
        (word,1)
      }
    }

    println(mapRdd.toDebugString)
    mapRdd.cache()
    println(mapRdd.toDebugString)
    //mapRdd.collect()


  }

}
