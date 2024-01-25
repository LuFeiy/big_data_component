package com.atguigu.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import org.junit.{After, Before, Test}

class KeyValueOperator {
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
  def partitionBy() : Unit = {
    val rdd = sc.makeRDD(Array((1, 'a'), (2, 'b'), (3, 'c')),3)
    printRdd(rdd,"rdd")
    printRdd(rdd.partitionBy(new HashPartitioner(2)),"partitionBy")
    printRdd(rdd.partitionBy(new MyPartitioner(2)),"MyPartitioner")
  }


  @Test
  def reduceByKey(): Unit = {
    val rdd = sc.makeRDD(List(('a', 1), ('b', 2), ('a', 4), ('b', 5)),2)
    printRdd(rdd.reduceByKey((x,y)=>(x+y)))
  }

  @Test
  def groupByKey(): Unit = {
    val rdd = sc.makeRDD(List(('a', 1), ('b', 2), ('a', 4), ('b', 5)),2)
    //将相同的key聚合到一个seq中
    printRdd(rdd.groupByKey())
    printRdd(rdd.groupByKey().map(t=>(t._1,t._2.sum)))
  }

  @Test
  def aggregateByKey(): Unit = {
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
    /*
    zeroValue: 用于每个分区初始化迭代的值
    seqOp:分区内的迭代函数它的两个参数，一个就是上一次的处理结果(第一次是zeroValue)，另外一个就是rdd中的值
    combOp:分区间的处理函数
      */
    //分区最大值的和
    val aggrdd = rdd.aggregateByKey(0)(math.max(_, _), _ + _)
    printRdd(rdd)
    printRdd(aggrdd)
  }


  //就是分区内和分区间完全一样
  @Test
  def foldByKey(): Unit = {
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 2), ("b", 4), ("b", 3), ("a", 6), ("a", 8)), 2)
    //全局取最大
    printRdd(rdd.foldByKey(0)(math.max))
    //求和
    printRdd(rdd.foldByKey(0)(_+_))
  }


  @Test
  def combineByKey(): Unit = {
    val list: List[(String, Int)] = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    val rdd: RDD[(String, Int)] = sc.makeRDD(list, 2)
    printRdd(rdd)

    val combineRdd = rdd.combineByKey(
      (_, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )

    printRdd(combineRdd)

    println(combineRdd.map{case (k,v) => (k,v._1/v._2)})

  }


  @Test
  def sortByKey(): Unit = {
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))
    printRdd(rdd.sortByKey())
  }


  @Test
  def mapValues(): Unit = {
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (1, "d"), (2, "b"), (3, "c")),3)
    printRdd(rdd.mapValues(_ + "|||"))
  }


  @Test
  def join(): Unit = {
    val rdd1: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")))

    val rdd2: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (4, 6)))

    printRdd(rdd1.join(rdd2))
  }


  @Test
  def cogroup(): Unit = {
    val rdd1: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")))

    val rdd2: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (4, 6)))

    printRdd(rdd1.cogroup(rdd2))
  }


}


class MyPartitioner(num: Int) extends Partitioner {
  // 设置的分区数
  override def numPartitions: Int = num

  // 具体分区逻辑
  override def getPartition(key: Any): Int = {

    if (key.isInstanceOf[Int]) {

      val keyInt: Int = key.asInstanceOf[Int]
      if (keyInt % 2 == 0)
        0
      else
        1
    }else{
      0
    }
  }
}



