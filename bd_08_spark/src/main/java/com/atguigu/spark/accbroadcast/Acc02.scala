package com.atguigu.spark.accbroadcast

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Acc02 {

  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2. Application：初始化一个SparkContext即生成一个Application；
    val sc: SparkContext = new SparkContext(conf)

    //3. 创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Hello", "Hello", "Hello", "Hello", "Spark", "Spark"))

    val sumAcc = new MyAccumulator()
    sc.register(sumAcc)


    rdd.foreach(
      num => {
        sumAcc.add(num)
      }
    )

    println(sumAcc)

    sc.stop()

  }

}


class MyAccumulator extends AccumulatorV2[String,mutable.Map[String,Long]]{

  var map = mutable.Map[String,Long]()
  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
    new MyAccumulator()
  }

  override def reset(): Unit = map.clear()

  override def add(v: String): Unit = {
    if(v.startsWith("H")){
      map(v) = map.getOrElseUpdate(v,0L) + 1L;
    }
  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
    var map1 = map
    var map2 = other.value

    map = map1.foldLeft(map2)(
      (map,kv)=>{
        map(kv._1) = map.getOrElseUpdate(kv._1,0L) + kv._2
        map
      }
    )
  }

  override def value: mutable.Map[String, Long] = map
}

