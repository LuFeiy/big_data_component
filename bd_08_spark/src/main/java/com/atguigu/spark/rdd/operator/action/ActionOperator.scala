package com.atguigu.spark.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

class ActionOperator {

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
  def reduce(): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val res = rdd.reduce(_ + _)
    print(res)
  }

  @Test
  def collect(): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val ints = rdd.collect()
    ints.foreach(println)
  }

  @Test
  def count(): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val res = rdd.count()
    print(res)
  }

  @Test
  def first(): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val res = rdd.first()
    print(res)
  }

  @Test
  def take(): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val res = rdd.take(2)
    res.foreach(println)
  }

  @Test
  def takeOrdered(): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val res = rdd.takeOrdered(2)
    res.foreach(println)
  }


  @Test
  def aggregate(): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),8)
    printRdd(rdd)
    print(rdd.aggregate(10)(_+_,_+_))
  }

  @Test
  def fold(): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 8)
    printRdd(rdd)
    print(rdd.fold(10)(_ + _))
  }


  @Test
  def countByKey(): Unit = {
    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2, "b"), (3, "c"), (3, "c")))
    val res = rdd.countByKey()

    print(res)
  }


  @Test
  def save(): Unit = {
    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2, "b"), (3, "c"), (3, "c")),2)
    rdd.saveAsTextFile("text")
    rdd.saveAsSequenceFile("hdfs")
    rdd.saveAsObjectFile("object")
  }


}
