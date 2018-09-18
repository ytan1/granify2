package com.assignment.spark

import org.apache.spark.sql.{Row, types}
import org.apache.spark.sql.types._

object AccessConvertUtils {

  val orderStruct = types.StructType(
    Array(
      StructField("st", StringType),
      StructField("start", StringType),
      StructField("revenue", DoubleType),
      StructField("siteId", IntegerType),
      StructField("ssid", StringType)
    )
  )
  val featureStruct = types.StructType(
    Array(
      StructField("st", StringType),
      StructField("start", StringType),
      StructField("feature1", DoubleType),
      StructField("feature2", DoubleType),
      StructField("feature3", DoubleType),
      StructField("feature4", DoubleType),
      StructField("ad", IntegerType),
      StructField("siteId", IntegerType),
      StructField("ssid", StringType)
    )
  )
  val sessionStruct = types.StructType(
    Array(
      StructField("st", StringType),
      StructField("start", StringType),
      StructField("browser", StringType),
      StructField("gr", IntegerType),
      StructField("source", StringType),
      StructField("siteId", IntegerType),
      StructField("ssid", StringType)
    )
  )

  def parseOrders(line: String) ={
    try {
      val splits = line.split(",")

      val st = TimeConvertUtil.parse(splits(0).substring(splits(0).indexOf(":") + 1))

      val revenue = splits(1).substring(splits(1).indexOf(":") + 1).toDouble

      val ssid = splits(2).substring(splits(2).indexOf(":") + 2, splits(2).lastIndexOf("\""))

      val siteId = splits(2).split(":")(2).toInt

      val temp = splits(2).split(":")(3)
      val start = TimeConvertUtil.parse(temp.substring(0, temp.indexOf("\"")))



      Row(st + ":00", start + ":00", revenue, siteId, ssid)

    }catch{
      case e:Exception => Row(0)
    }
  }

  def parseFeatures(line: String) ={
    try {

      val splits = line.split(",")
      var ad:Int = -2
      if(line.indexOf("ad") > -1){
        ad = splits(2).substring(splits(2).indexOf(":") + 1).replace("\"", "").toInt //"-1", "1", "2" ...
      }else{
        ad = -2
        println("\\u001B[32m Find feature row without ad info \\u001B[0m")
      }

      val st = TimeConvertUtil.parse(splits(0).substring(splits(0).indexOf(":") + 1))

      val feature1 = splits(5).substring(splits(5).indexOf(":") + 1).toDouble

      val feature2 = splits(4).substring(splits(4).indexOf(":") + 1).toDouble

      val feature3 = splits(3).substring(splits(3).indexOf(":") + 1).toDouble

      val feature4 = splits(1).substring(splits(1).indexOf(":") + 1).toDouble



      val ssid = splits(6).substring(splits(6).indexOf(":") + 2, splits(6).lastIndexOf("\""))

      val siteId = splits(6).split(":")(2).toInt

      val temp = splits(6).split(":")(3)
      val start = TimeConvertUtil.parse(temp.substring(0, temp.indexOf("\"")))


      Row(st + ":00", start + ":00", feature1, feature2, feature3, feature4, ad, siteId, ssid)

    }catch{
      case e:Exception => Row(0)
    }
  }

  def parseSessions(line: String) ={
    try {

      if(line.indexOf("browser") > -1){
        val splits = line.split(",")

        val st = TimeConvertUtil.parse(splits(0).substring(splits(0).indexOf(":") + 1))

        val browser = splits(1).substring(splits(1).indexOf(":") + 1).replace("\"", "")

        val gr = splits(2).substring(splits(2).indexOf(":") + 1).toInt

        val source = splits(3).substring(splits(3).indexOf(":") + 1)

        val ssid = splits(4).substring(splits(4).indexOf(":") + 2, splits(4).lastIndexOf("\""))

        val siteId = splits(4).split(":")(2).toInt

        val temp = splits(4).split(":")(3)

        val start = TimeConvertUtil.parse(temp.substring(0, temp.indexOf("\"")))


//        println(Row(st, start, browser, gr, source,  siteId, ssid))
        Row(st + ":00", start + ":00", browser, gr, source,  siteId, ssid)
      }else{
        val splits = line.split(",")

        val st = TimeConvertUtil.parse(splits(0).substring(splits(0).indexOf(":") + 1))

        val browser = "Unknown"

        val gr = splits(1).substring(splits(1).indexOf(":") + 1).toInt

        val source = splits(2).substring(splits(2).indexOf(":") + 1)

        val ssid = splits(3).substring(splits(3).indexOf(":") + 2, splits(3).lastIndexOf("\""))

        val siteId = splits(3).split(":")(2).toInt

        val temp = splits(3).split(":")(3)

        val start = TimeConvertUtil.parse(temp.substring(0, temp.indexOf("\"")))


//        println(Row(st, start, browser, gr, source,  siteId, ssid))
        Row(st + ":00", start + ":00", browser, gr, source,  siteId, ssid)
      }


    }catch{
      case e:Exception => Row(0)
    }
  }
}
