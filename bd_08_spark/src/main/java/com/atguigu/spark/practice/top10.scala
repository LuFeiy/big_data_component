package com.atguigu.spark.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object top10 {
  def printRdd[T](rdd: org.apache.spark.rdd.RDD[T]): Unit = {
    val partitionedData = rdd.mapPartitionsWithIndex { (index, partition) =>
      val partitionContent = partition.map(_.toString).mkString(", ") // 将分区数据转换为逗号分隔的字符串
      Iterator(s"Partition $index: $partitionContent") // 将分区号和分区数据合并到一行中
    }

    partitionedData.collect().foreach(println)
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("top10").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //读取数据
    val inputRdd = sc.textFile("bd_08_spark/input/user_visit_action.txt")
    //转换数据
    val userRdd = inputRdd.map(line => {
      val fields = line.split("_")
      UserVisitAction(
        fields(0), fields(1).toLong, fields(2), fields(3).toLong,
        fields(4), fields(5), fields(6), fields(7).toLong,
        fields(8), fields(9), fields(10), fields(11), fields(12).toLong
      )
    })

    //继续转换得到（品类，点击1）
    val clickRdd:RDD[(String,Int)] = userRdd
      .filter(u=>{
      (u.click_category_id != -1)
      })
      .map(u => (u.click_category_id, 1))
      .reduceByKey(_+_)


    val orderRdd:RDD[(String,Int)] = userRdd
      .filter(u => {
        (u.order_category_ids != null)
      })
      .map(u => u.order_category_ids)
      .flatMap(x=>x.split(","))
      .map((_,1))
      .reduceByKey(_+_)

    val payRdd:RDD[(String,Int)] = userRdd
      .filter(u => {
        (u.pay_category_ids != null)
      })
      .map(u => u.pay_category_ids)
      .flatMap(x => x.split(","))
      .map((_, 1))
      .reduceByKey(_ + _)


    //val clickRdd = userRdd.map(u => (u.click_category_id, 1))
    //printRdd(clickRdd)
    //join

    val mergeRdd = clickRdd.join(orderRdd).join(payRdd)
    mergeRdd
    printRdd(mergeRdd)

    val resRDD = mergeRdd.sortBy(x => (x._2._1._1, x._2._1._2, x._2._2)).top(10)

    //printRdd(resRDD)
    print(resRDD.mkString("Array(", ", ", ")"))

    sc.stop()

  }
}


//用户访问动作表
//用户访问动作表
case class UserVisitAction(
                            date: String,//用户点击行为的日期
                            user_id: Long,// 用 户 的 ID
                            session_id: String,//Session 的 ID
                            page_id: Long,// 某 个 页 面 的 ID
                            action_time: String,//动作的时间点
                            search_keyword: String,//用户搜索的关键词
                            click_category_id: String,// 某 一 个 商 品 品 类 的 ID
                            click_product_id: Long,// 某 一 个 商 品 的 ID
                            order_category_ids: String,//一次订单中所有品类的 ID 集合
                            order_product_ids: String,//一次订单中所有商品的 ID 集合
                            pay_category_ids: String,//一次支付中所有品类的 ID 集合
                            pay_product_ids: String,//一次支付中所有商品的 ID 集合
                            city_id: Long )//城市 id

