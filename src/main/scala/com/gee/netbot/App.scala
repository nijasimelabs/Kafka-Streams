package com.gee.netbot

import com.typesafe.scalalogging.Logger
import java.util.Properties

import Constants._
import com.gee.netbot.utils.ConfigManager

/*
 * Every stream app should extend this
 */
abstract class App {

  /*
   * To be overridden
   */
  def main(args: Array[String]): Unit

  /*
   * Application Name
   */
  def getAppName(): String

  /*
   * Logger instance
   */
  protected val logger = Logger(this.getClass.getName)

  ConfigManager.setLogger(logger)

  protected def getConfig(key: String): String = {
    return ConfigManager.getConfig(key)
  }

  /*
   * Return Stream application configurations for app @name
   */
  def getAppProperties(): Option[Properties] = {
    val path = getAppName() + "/" + APP_CONFIG_FILE
    return ConfigManager.getProperties(path)
  }

  /*
   * Return producer configurations for app @name
   */
  def getProducerProperties(): Option[Properties] = {
    val path = getAppName() + "/" + PROD_CONFIG_FILE
    return ConfigManager.getProperties(path)
  }

  /*
   * Return consumer configurations for app @name
   */
  def getConsumerProperties(): Option[Properties] = {
    val path = getAppName() + "/" + CONS_CONFIG_FILE
    return ConfigManager.getProperties(path)
  }
}
