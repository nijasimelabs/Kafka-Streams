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

  /*
   * Application level configurations
   */
  private val config = getAppConfigs() match {
    case Some(cfg) => cfg
    case None => {
      logger.error("Could not load App configurations")
      // TODO: should we quit ?
      new Properties()
    }
  }

  protected def getConfig(key: String): String = {
    return this.config.getProperty(key, "")
  }

  /*
   * Return application level configurations for app @name
   */
  def getAppConfigs(): Option[Properties] = {
    return ConfigManager.getProperties(CONFIG_FILE)
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
