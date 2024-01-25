package com.atguigu.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

class DoubleValueOperator {
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
  def union():Unit = {
    val rdd1: RDD[Int] = sc.makeRDD(1 to 4,2)
    val rdd2: RDD[Int] = sc.makeRDD(4 to 7,2)
    printRdd(rdd1,"rdd1")
    printRdd(rdd2,"rdd2")
    //并不会去重，看结果就是分区级别的聚合
    printRdd(rdd1.union(rdd2),"union")
  }

  @Test
  def subtract() : Unit = {
    val rdd1: RDD[Int] = sc.makeRDD(1 to 4, 2)
    val rdd2: RDD[Int] = sc.makeRDD(4 to 7, 2)
    printRdd(rdd1, "rdd1")
    printRdd(rdd2, "rdd2")
    //全局sub,会发生shuffle
    printRdd(rdd1.subtract(rdd2), "subtract")
  }

  @Test
  def intersection(): Unit = {
    val rdd1: RDD[Int] = sc.makeRDD(1 to 4, 2)
    val rdd2: RDD[Int] = sc.makeRDD(4 to 7, 2)
    printRdd(rdd1, "rdd1")
    printRdd(rdd2, "rdd2")
    //全局sub,会发生shuffle
    printRdd(rdd1.intersection(rdd2), "intersection")
  }


  //分区和分区内的元素都要一一对应
  @Test
  def zip(): Unit = {
    val rdd1: RDD[Int] = sc.makeRDD(Array(1,2,3,4),2)
    val rdd2: RDD[Char] = sc.makeRDD(Array('a','b','c','d'),2)
    printRdd(rdd1, "rdd1")
    printRdd(rdd2, "rdd2")
    //全局sub,会发生shuffle
    printRdd(rdd1.zip(rdd2), "intersection")
  }











}
