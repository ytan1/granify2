package com.assignment.spark

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer


object StatDAO {


  def insertAnalyze1Res(list: ListBuffer[Analyze1Object]): Unit ={
    var connection:Connection = null
    var ps: PreparedStatement = null

    try{
      connection = MySQLUtils.getConnections()

      connection.setAutoCommit(false)

      val sql = "insert into table1(start, siteId, gr, ad, browser, sessions, conversions, transactions, revenues) values (?,?,?,?,?,?,?,?,?)"

      ps = connection.prepareStatement(sql)

      for(ele <- list){

        ps.setString(1, ele.start)
        ps.setInt(2, ele.siteId)
        ps.setInt(3, ele.gr)
        ps.setInt(4, ele.ad)
          ps.setString(5, ele.browser)
          ps.setLong(6, ele.sessions)
          ps.setLong(7, ele.conversions)
          ps.setLong(8, ele.transactions)
          ps.setDouble(9, ele.revenues)
          ps.addBatch()
        }
        ps.executeBatch()
        connection.commit()
      }catch {
        case e:Exception => e.printStackTrace()
      }finally {
        MySQLUtils.release(connection, ps)
      }


  }
  def insertAnalyze2Res(list: ListBuffer[Analyze2Object]): Unit ={
    var connection:Connection = null
    var ps: PreparedStatement = null

    try{
      connection = MySQLUtils.getConnections()

      connection.setAutoCommit(false)

      val sql = "insert into table2(siteId, ad, mean1, sd1, mean2, sd2, mean3, sd3, mean4, sd4) values (?,?,?,?,?,?,?,?,?,?)"

      ps = connection.prepareStatement(sql)

      for(ele <- list){

        ps.setInt(1, ele.siteId)
        ps.setInt(2, ele.ad)
        ps.setDouble(3, ele.mean1)
        ps.setDouble(4, ele.sd1)
        ps.setDouble(5, ele.mean2)
        ps.setDouble(6, ele.sd2)
        ps.setDouble(7, ele.mean3)
        ps.setDouble(8, ele.sd3)
        ps.setDouble(9, ele.mean4)
        ps.setDouble(10,ele.sd4)
        ps.addBatch()
      }
      ps.executeBatch()
      connection.commit()
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      MySQLUtils.release(connection, ps)
    }


  }
  def deleteTable1(): Unit ={
    val tables = Array("table1")
    var connection:Connection = null
    var ps: PreparedStatement = null
    try{
      connection = MySQLUtils.getConnections()

      for(table <- tables){
        val sql = s"delete from $table"
        ps = connection.prepareStatement(sql)
        ps.executeUpdate()
      }

    }catch {
      case e:Exception => e.printStackTrace()
    }
    finally{
      MySQLUtils.release(connection, ps)
    }
  }
  def deleteTable2(): Unit ={
    val tables = Array("table2")
    var connection:Connection = null
    var ps: PreparedStatement = null
    try{
      connection = MySQLUtils.getConnections()

      for(table <- tables){
        val sql = s"delete from $table"
        ps = connection.prepareStatement(sql)
        ps.executeUpdate()
      }

    }catch {
      case e:Exception => e.printStackTrace()
    }
    finally{
      MySQLUtils.release(connection, ps)
    }
  }
}
