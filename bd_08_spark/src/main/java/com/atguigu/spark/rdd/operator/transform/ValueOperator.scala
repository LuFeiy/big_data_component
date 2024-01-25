package com.atguigu.spark.rdd.operator.transform

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

class ValueOperator {
  var conf: SparkConf = null

  var sc: SparkContext = null

  @Before
  def confPrepare: Unit = {
    conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

  }


  @Test
  def map():Unit = {
    val rdd: RDD[Int] = sc.makeRDD(1 to 4, 2)

    val mapRdd:RDD[Int]   = rdd.map(x => x * 2)

    mapRdd.collect().foreach(println)
  }

  @Test
  def mapPartition(): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(1 to 4, 2)

    val mapRdd: RDD[Int] = rdd.mapPartitions(x=>x.map(_ * 2))

    mapRdd.collect().foreach(println)
  }


  @Test
  def mapPartitionWithIndex(): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(1 to 4, 2)

    val mapRdd: RDD[(Int,Int)] = rdd.mapPartitionsWithIndex((index,items)=>{items.map((index,_))})
    //只对分区1翻倍
    val mapRdd2: RDD[(Int,Int)] = rdd.mapPartitionsWithIndex((index,items)=>{
      if (index == 0){
        items.map((index,_))
      }else{
        //items.map(x=>(index,_))
        items.map(x => (index, x*2)) // 明确指定 x 的类型
      }
    })

    mapRdd2.collect().foreach(println)
  }


  @Test
  def flatMap():Unit = {
    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1,2),List(3,4),List(5,6),List(7)))

    //a=>a或者说 _ 的意思是不做任何操作,原样返回。 由flatMap来自动处理迭代器或集合
    val flatRdd = rdd.flatMap(a=>a)

    flatRdd.collect().foreach(println)
  }


  @Test
  def glom(): Unit = {
    val rdd = sc.makeRDD(1 to 4, 2)

    val rdd1 = rdd.glom()
    rdd1.collect().foreach(println)

    val rdd2 = rdd1.map(_.max)
    rdd2.collect().foreach(println)

  }


  @Test
  def groupBy() : Unit = {
    val rdd = sc.makeRDD(1 to 4, 2)
    rdd.groupBy(_%2).collect().foreach(println)

    val rddStr: RDD[String] = sc.makeRDD(List("hello","hive","hadoop","spark","scala"))
    rddStr.groupBy(s=>s.substring(0,1)).collect().foreach(println)

    //wordCount
    var wcRdd =  sc.makeRDD(List("Hello Scala", "Hello Spark", "Hello World"))

    val wordRdd = wcRdd.flatMap(x => x.split(" "))

    val mapRdd = wordRdd.map(x => (x, 1))

    val groupRdd = mapRdd.groupBy(x => (x._1))

    var wordToSum = groupRdd.map{
      case(word,list)=>(word,list.size)
    }

    wordToSum.collect().foreach(println)

  }


  @Test
  def filter() : Unit = {
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4),2)
    rdd.filter(_ % 2 == 0).collect().foreach(println)
  }


  @Test
  def sample() : Unit = {
    val rdd: RDD[Int] = sc.makeRDD(1 to 10)
    //放回,可能重复
    rdd.sample(false,0.3,5).collect()foreach(println)
    //不放回,无重复值
    rdd.sample(true,0.3,5).collect()foreach(println)
  }


  @Test
  def distinct(): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,1,5,2,9,6,1))

    //默认情况下分区数和原先的保持一致
    rdd.distinct().collect().foreach(println)

    //指定分区数
    rdd.distinct(2).collect().foreach(println)

  }


  @Test
  def coalesce():Unit = {
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 4)
    printRdd(rdd)

    //缩减分区
    val coalescedRdd = rdd.coalesce(2)
    printRdd(coalescedRdd)


    val rdd1: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 3)
    //当shuffle用默认值false,分区数大于原先分区则失效,所以一半是用于缩减分区
    printRdd(rdd1.coalesce(4))
    //执行shuffle
    printRdd(rdd1.coalesce(4,true))

  }

  @Test
  def repartition() : Unit = {
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 3)
    //函数就一行代码：coalesce(numPartitions, shuffle = true)
    printRdd(rdd.repartition(2))
  }


  @Test
  def sortBy(): Unit = {
    val rdd: RDD[Int] =  sc.makeRDD(List(2, 1, 4, 3, 6, 5),2)
    printRdd(rdd)
    //会发生shuffle
    printRdd(rdd.sortBy(x=>x))
  }





  @After
  def stopConf: Unit = {
    sc.stop()
  }


  def printRdd[T](rdd: org.apache.spark.rdd.RDD[T]): Unit = {
    val partitionedData = rdd.mapPartitionsWithIndex { (index, partition) =>
      Iterator(s"Partition $index:") ++ partition.map(_.toString)
    }

    partitionedData.collect().foreach(println)
  }
}
