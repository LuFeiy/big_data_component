package com.atguigu.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.{After, Before, Test}

class dataframe {
  var spark: SparkSession  = null
  var sc: SparkContext = null

  @Before
  def confPrepare: Unit = {


    spark = SparkSession.builder()
      .master("local[*]")
      .appName("Word Count")
      .getOrCreate()

    sc = spark.sparkContext

  }

  @After
  def stopConf: Unit = {
    sc.stop()
  }


  @Test
  def test_01(): Unit = {
    val rdd: RDD[(String, Int)] = sc.parallelize(Array(("lisi", 10), ("zs", 20), ("zhiling", 40)))
    val rddRow = rdd.map(x => Row(x._1, x._2))
    val types = StructType(Array(StructField("name",StringType),StructField("age",IntegerType)))
    val df:DataFrame = spark.createDataFrame(rddRow, types)
    df.show

  }


}
