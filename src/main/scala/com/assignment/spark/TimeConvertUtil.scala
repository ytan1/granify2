package com.assignment.spark

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

object TimeConvertUtil {
  //hourly granularity ??????
  val Target_Format = FastDateFormat.getInstance("yyyy-MM-dd HH", Locale.ENGLISH)

  def parse(time:String) = {
    Target_Format.format(new Date((time + "000").toLong))
  }

//  def main(args: Array[String]): Unit = {
////    System.out.println(Target_Format.format(new Date(("1464739228000").toLong)))
//    println(parse("1464739228"))
//  }


}
