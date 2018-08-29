package com.gee.netbot.dao

import java.sql.{Connection, DriverManager, ResultSet}

import com.gee.netbot.utils.ConfigManager
import com.gee.netbot.Constants._

object DataSource {
  val url = {
    val host = ConfigManager.getConfig(DB_HOST)
    val port = ConfigManager.getConfig(DB_PORT)
    val dbname = ConfigManager.getConfig(DB_DBNAME)
    s"jdbc:mysql://$host:$port/$dbname"
  }

  val username = ConfigManager.getConfig(DB_USER)
  val driver = ConfigManager.getConfig(DB_DRIVER)
  val password = ConfigManager.getConfig(DB_PASSWORD)
  var connection: Connection = null

  def init() = {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
  }

  def excecute(query: String): ResultSet = {
    var result: ResultSet = null

    try {
      if (connection == null) {
        init()
      }
      val statement = connection.createStatement
      result = statement.executeQuery(query)
    } catch {
      case e: Exception => {
        // logger here
      }
    }

    return result
  }

  def close() = {
    try {
      if (connection != null) {
        connection.close
      }
    } catch {
      case e: Exception => {
        // log error here
      }
    }
  }
}
