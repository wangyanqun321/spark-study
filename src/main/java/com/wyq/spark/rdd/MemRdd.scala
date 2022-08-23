package com.wyq.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object MemRdd {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc: SparkContext = new SparkContext(sparkConf)
    val rdd1 = sc.parallelize(List(1, 2, 3, 4))
    val rdd2 = sc.makeRDD(List(1, 2, 3, 4))
    rdd1.collect().foreach(println)
    rdd2.collect().foreach(println)
  }

}
