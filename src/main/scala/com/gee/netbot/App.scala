package com.gee.netbot

import com.typesafe.scalalogging.Logger
import java.util.Properties
import java.io.FileInputStream
import java.io.IOException
import java.io.InputStream

import Constants._

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
   * Return location of configuration files.
   * This is set by passing <code>-Dconfig.path=/path/to/config/dir</code>
   * to jvm.
   */
  private def getConfigPath(): String = {
    val configPath = System.getProperty(CONFIG_PATH)
    if (configPath == null || configPath == "") {
      logger.error("Config path is not set")
      throw new Exception("Config path is not set")
    }

    if (configPath.endsWith("/")) {
      return configPath
    }

    return configPath + "/"
  }

  /*
   * Read properties from given filepath
   */
  private def getPropertiesFromFile(path: String): Option[Properties] = {
    var result: Option[Properties] = None
    val props = new Properties()
    var input: FileInputStream = null

    try {
      input = new FileInputStream(path)
      props.load(input)
      result = Some(props)
    } catch {
      // TODO logging
      case e: IOException => {
        logger.error("Error occured reading properties: {}", e.getMessage)
        throw e
      }
      case e: Exception => {
        logger.error("Error occured reading properties: {}", e)
        throw e
      }
    } finally {
      if (input != null) {
        input.close()
      }
    }

    return result
  }

  /*
   * Return application level configurations for app @name
   */
  def getAppConfigs(): Option[Properties] = {
    val path = getConfigPath() + CONFIG_FILE
    logger.debug("Loading application configuration from :{}", path)
    return getPropertiesFromFile(path)
  }


  /*
   * Return Stream application configurations for app @name
   */
  def getAppProperties(): Option[Properties] = {
    val path = getConfigPath() + getAppName() + "/" + APP_CONFIG_FILE
    logger.debug("Loading stream configuration from :{}", path)
    return getPropertiesFromFile(path)
  }

  /*
   * Return producer configurations for app @name
   */
  def getProducerProperties(): Option[Properties] = {
    val path = getConfigPath() + getAppName() + "/" + PROD_CONFIG_FILE
    logger.debug("Loading producer configuration from :{}", path)
    return getPropertiesFromFile(path)
  }

  /*
   * Return consumer configurations for app @name
   */
  def getConsumerProperties(): Option[Properties] = {
    val path = getConfigPath() + getAppName() + "/" + CONS_CONFIG_FILE
    logger.debug("Loading consumer configuration from :{}", path)
    return getPropertiesFromFile(path)
  }
}
