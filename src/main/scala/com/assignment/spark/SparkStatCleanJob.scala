package com.assignment.spark

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkStatCleanJob {

  def main(args: Array[String]): Unit ={
    System.setProperty("hadoop.home.dir", "D:/hadoop-2.6.2")
      cleanOrders()
      cleanFeatures()
      cleanSessions()
  }
  def cleanOrders(): Unit ={
    val spark = SparkSession.builder().appName("SparkStatCleanJob1")
//      .config("spark.sql.parquet.compression.codec", "gzip")
      .master("local[2]")
      .getOrCreate()

    val rDD = spark.sparkContext.textFile("file:///C:/Users/ytan1/Desktop/Granify/granify22/data/orders")

//    rDD.take(10).foreach(println)

    val dataFrame = spark.createDataFrame(rDD.map(line => AccessConvertUtils.parseOrders(line)), AccessConvertUtils.orderStruct)
    dataFrame.show(10, false)
    dataFrame.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
      .save("file:///C:/Users/ytan1/Desktop/Granify/granify22/cleanData/orders")

    spark.stop()
  }

  def cleanFeatures(): Unit ={
    val spark = SparkSession.builder().appName("SparkStatCleanJob2")
      //      .config("spark.sql.parquet.compression.codec", "gzip")
      .master("local[2]")
      .getOrCreate()

    val rDD = spark.sparkContext.textFile("file:///C:/Users/ytan1/Desktop/Granify/granify22/data/features")

//        rDD.take(10).foreach(println)

    val dataFrame = spark.createDataFrame(rDD.map(line => AccessConvertUtils.parseFeatures(line)), AccessConvertUtils.featureStruct)
        dataFrame.show(10, false)
    dataFrame.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
      .save("file:///C:/Users/ytan1/Desktop/Granify/granify22/cleanData/features")

    spark.stop()
  }

  def cleanSessions():Unit = {
    val spark = SparkSession.builder().appName("SparkStatCleanJob3")
      //      .config("spark.sql.parquet.compression.codec", "gzip")
      .master("local[2]")
      .getOrCreate()

    val rDD = spark.sparkContext.textFile("file:///C:/Users/ytan1/Desktop/Granify/granify22/data/sessions")

//            rDD.take(10).foreach(println)

    val dataFrame = spark.createDataFrame(rDD.map(line => AccessConvertUtils.parseSessions(line)), AccessConvertUtils.sessionStruct)
    dataFrame.show(10, false)
    dataFrame.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
      .save("file:///C:/Users/ytan1/Desktop/Granify/granify22/cleanData/sessions")

    spark.stop()
  }

}
