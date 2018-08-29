package com.gee.netbot.utils

import com.typesafe.scalalogging.Logger
import java.io.FileInputStream
import java.io.IOException
import java.io.InputStream
import java.util.Properties

import com.gee.netbot.Constants._

object ConfigManager {
  private var logger = Logger(this.getClass.getName)

  private val configPath = getConfigPath()

  /*
   * Update logger to that of App instance
   */
  def setLogger(lgr: Logger) = {
    this.logger = lgr
  }

  /*
   * Return location of configuration files.
   * This is set by passing <code>-Dconfig.path=/path/to/config/dir</code>
   * to jvm.
   */
  def getConfigPath(): String = {
    val configPath = System.getProperty(CONFIG_PATH)
    if (configPath == null || configPath == "") {
      logger.error("Config path is not set")
      throw new Exception("Config path is not set")
    }

    if (configPath.endsWith("/")) {
      logger.info("Config Path set to: {}", configPath)
      return configPath
    }

    logger.info("Config Path set to: {}", configPath + "/")
    return configPath + "/"
  }

  def getProperties(name: String): Option[Properties] = {
    return getPropertiesFromFile(this.configPath + name)
  }

  /*
   * Read properties from given filepath
   */
  def getPropertiesFromFile(path: String): Option[Properties] = {
    logger.info("Reading propertiess from: {}", path)
    var result: Option[Properties] = None
    val props = new Properties()
    var input: FileInputStream = null

    try {
      input = new FileInputStream(path)
      props.load(input)
      result = Some(props)
    } catch {
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
}
