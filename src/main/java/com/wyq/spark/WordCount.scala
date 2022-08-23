package com.wyq.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    // 创建 Spark 上下文环境对象(连接对象)
    val sc: SparkContext = new SparkContext(sparkConf)
    // 读取文件数据
    val result = sc.textFile("/Users/yanqunwang/IdeaProjects/spark-study/src/main/resources/data/wordcount.txt")
      // 将文件中的数据进行分词
      .flatMap(_.split(","))
      // 转换数据结构 word => (word, 1)
      .map((_, 1))
      // 将转换结构后的数据按照相同的单词进行分组聚合
      .reduceByKey(_ + _)
      // 将数据聚合结果采集到内存中
      .collect()
    // 打印结果
    result.foreach(println)
    //关闭 Spark 连接
    sc.stop()
  }
}
