package com.wyq.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object Map {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.parallelize(List(1, 2, 3, 4))
      .map(_ * 2)
      .collect().foreach(println)
  }

}
