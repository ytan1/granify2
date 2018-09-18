package com.assignment.spark

import java.sql.{Connection, DriverManager, PreparedStatement}

object MySQLUtils {
  def getConnections() = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/granify_spark?characterEncoding=utf-8&user=root&password=root")
  }

  def release(connection: Connection, ps: PreparedStatement): Unit ={
    try{
      if(ps != null){
        ps.close()
      }
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      if(connection != null){
        connection.close()
      }
    }
  }


}
