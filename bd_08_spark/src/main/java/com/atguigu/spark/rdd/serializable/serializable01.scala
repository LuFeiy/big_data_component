package com.atguigu.spark.rdd.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object serializable01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(conf)


    val user1 = new User()
    user1.name = "zhangsan"

    val user2 = new User()
    user2.name = "lisi"

    val userRDD: RDD[User] = sc.makeRDD(List(user1, user2))

    //不实现Serializable，下面三两行都会报错,也就是说RDD中的对象必须序列化
    userRDD.foreach(println)
    userRDD.foreach(u=>println(u.name))
    println(userRDD.count())

    sc.stop()
  }
}

class User extends Serializable {
  var name: String = _
}


