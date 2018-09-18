package com.assignment.spark

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

object SparkStatAnalyzeJob {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:/hadoop-2.6.2")
    val spark = SparkSession.builder().appName("SparkAnalyzeJob")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled","false")
      .master("local[2]").getOrCreate()

    val ordersDF = spark.read.format("parquet").load("file:///C:/Users/ytan1/Desktop/Granify/granify22/cleanData/orders")
    val featuresDF = spark.read.format("parquet").load("file:///C:/Users/ytan1/Desktop/Granify/granify22/cleanData/features")
    val sessionsDF = spark.read.format("parquet").load("file:///C:/Users/ytan1/Desktop/Granify/granify22/cleanData/sessions")

//    ordersDF.show(10, false)
//    featuresDF.show(10, false)
//    sessionsDF.show(10, false)

    analyze1(spark, ordersDF, featuresDF, sessionsDF)
//    analyze2(spark, featuresDF)
    spark.stop()
  }

  //Session start date at hourly granularity, site_id, gr, Ad, browser, number of sessions,
  //number of conversions, number of transactions, sum of revenue
  def analyze1(spark: SparkSession, ordersDF: DataFrame, featuresDF: DataFrame, sessionsDF: DataFrame): Unit ={

    ordersDF.createOrReplaceTempView("orders")
    featuresDF.createOrReplaceTempView("features")
    sessionsDF.createOrReplaceTempView("sessions")

    //To get some info about quality of the original data
//    spark.sql("select count(1) as num_sessions from sessions").show(false)   //total session 100969 before removing duplicate
//    spark.sql("select count(1) as browser_unknown from sessions where browser = \"Unknown\"").show(false) //session without browser info 952 before removing duplicate
//    spark.sql("select count(1) as unrecorded_order from orders where not exists (select 1 from sessions where sessions.ssid = orders.ssid)").show(false) //4017 orders with ssid not recorded in sessions table
//    spark.sql("select count(1) as unrecorded_feature from features where not exists (select 1 from sessions where sessions.ssid = features.ssid)").show(false) // 101 features with ssid not recorded in sessions tables
//    spark.sql("select count(1) as session_without_feature from sessions where not exists (select 1 from features where sessions.ssid = features.ssid)").show(false) //1030 sessions without feature info

//    spark.sql("select * from sessions where ssid in (select ssid from sessions group by ssid having count(distinct start) > 1) order by ssid").show(200, false)   //no distinct start with duplicate ssid
//    spark.sql("select count(*) from (select count(1) from sessions group by ssid having count(1) > 1)").show(200, false)    //result = 1561, 1561 duplicate session rows, need to remove

    //To remove rows with duplicate ssid in sessions table for further cal.

    spark.sql("select first_value(st) as st,first_value(start) as start,first_value(browser) as browser,first_value(gr) as gr,first_value(source) as source,first_value(siteId) as siteId, ssid from sessions group by ssid order by st").createOrReplaceTempView("distinct_sessions")
    spark.sql("select count(1) as sessions_num from distinct_sessions").show(false) //result = 99397 after remove duplicate session

    //To count the conversion with single transaction
//    val totalDF = spark.sql("select sum(revenue) as total, first_value(start) as start, ssid from orders group by ssid")
//    totalDF.createOrReplaceTempView("totals")
//    spark.sql("select count(1) from (select sum(revenue) as total, first_value(start) as start, ssid from orders group by ssid having count(1) > 1)").show(1000) // result = 4344
//    spark.sql("select sum(revenue) as total, first_value(start) as start, ssid from orders group by ssid having count(1) = 1").show(1000, false)
//    spark.sql("select count(1) from totals").show(false) //result = 4829
////    conversion with single transaction 485

    //join features and distinct_sessions first
    spark.sql("select distinct_sessions.start as start," +
      "distinct_sessions.browser as browser, distinct_sessions.gr as gr," +
      "distinct_sessions.siteId as siteId, features.ad as ad, " +
      "distinct_sessions.ssid as ssid " +
      "from distinct_sessions left JOIN features ON distinct_sessions.ssid = features.ssid").createOrReplaceTempView("fusion")



    val tempDF = spark.sql("" +
                            "select first_value(f.start) as start, first_value(f.gr) as gr, first_value(f.browser) as browser, first_value(f.siteId) as siteId, first_value(case when f.ad is null then -3 else f.ad end) as ad, f.ssid as ssid, " +
                            "sum(case when o.revenue is null then 0 else o.revenue end) as revenue, " +
                            "count(o.revenue) as transactions " +
                            "from fusion as f " +
                            "left join orders as o on f.ssid = o.ssid " +
                            "group by f.ssid " +
                            "order by f.ssid")
//    tempDF.show(2000, false)
    tempDF.createOrReplaceTempView("temp")
    val resultDF = spark.sql("" +
                              "select start, siteId, gr, ad, browser, " +
                              "count(1) as sessions, " +
                              "sum(case when revenue > 0 then 1 else 0 end) as conversions, " +
                              "sum(transactions) as transactions, " +
                              "sum(revenue) as revenues " +
                              "from temp " +
                              "group by start, siteId, gr, ad, browser " +
                              "order by start")

      resultDF.show(1000,false)
//      resultDF.createOrReplaceTempView("result")
//      spark.sql("select sum(transactions) from result").show(10)   //result 28778 total transactions with sessions recorded
    // something need to optimize here
//      spark.sql("select sum(conversions) from result").show(10)   //result 4263  total conversions with sessions recorded,  566 conversions are not recorded with sessions info

    //tsv format ouput
//    resultDF.write.option("delimiter", "\t").mode(SaveMode.Overwrite).csv("file:///C:/Users/ytan1/Desktop/Granify/granify22/tsv/analyze1")
//
//    //insert results into local mysql
//    StatDAO.deleteTable1()
//    try{
//      resultDF.foreachPartition(paritition => {
//        val list = new ListBuffer[Analyze1Object]
//
//        paritition.foreach(row => {
//          val start = row.getAs[String]("start")
//          val ad = row.getAs[Int]("ad")
//          val browser = row.getAs[String]("browser")
//          val siteId = row.getAs[Int]("siteId")
//          val gr = row.getAs[Int]("gr")
//          val sessions = row.getAs[Long]("sessions")
//          val conversions = row.getAs[Long]("conversions")
//          val transactions = row.getAs[Long]("transactions")
//          val revenues = row.getAs[Double]("revenues")
//          list.append(Analyze1Object(start, siteId,gr,ad,browser,sessions,conversions,transactions, revenues))
//        })
//        StatDAO.insertAnalyze1Res(list)
//      })
//    }catch {
//      case e: Exception => e.printStackTrace()
//    }
  }

  def analyze2(spark: SparkSession, featuresDF: DataFrame): Unit ={
    featuresDF.createOrReplaceTempView("features")
    val resDF = spark.sql("select siteId, ad, " +
                      "avg(feature1) as mean1, stddev(feature1) as sd1, " +
                      "avg(feature2) as mean2, stddev(feature2) as sd2, " +
                      "avg(feature3) as mean3, stddev(feature3) as sd3, " +
                      "avg(feature4) as mean4, stddev(feature4) as sd4 " +
                      "from features group by siteId, ad")
    resDF.show(5000, false)
    resDF.write.option("delimiter", "\t").mode(SaveMode.Overwrite).csv("file:///C:/Users/ytan1/Desktop/Granify/granify22/tsv/analyze2")

    //insert results into local mysql
    StatDAO.deleteTable2()
    try{
      resDF.foreachPartition(paritition => {
        val list = new ListBuffer[Analyze2Object]

        paritition.foreach(row => {
          val siteId = row.getAs[Int]("siteId")
          val ad = row.getAs[Int]("ad")
          val mean1 = row.getAs[Double]("mean1")
          val sd1 = row.getAs[Double]("sd1")
          val mean2 = row.getAs[Double]("mean2")
          val sd2 = row.getAs[Double]("sd2")
          val mean3 = row.getAs[Double]("mean3")
          val sd3= row.getAs[Double]("sd3")
          val mean4= row.getAs[Double]("mean4")
          val sd4 = row.getAs[Double]("sd4")
          list.append(Analyze2Object(siteId, ad, mean1, sd1, mean2, sd2, mean3, sd3, mean4,sd4))
        })
        StatDAO.insertAnalyze2Res(list)
      })
    }catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
