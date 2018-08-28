package com.gee.netbot

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
   * Application level configurations
   */
  private val config = getAppConfigs() match {
    case Some(cfg) => cfg
    case None => new Properties()
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
      case e: IOException =>
        throw e
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
    return getPropertiesFromFile(getConfigPath() + CONFIG_FILE)
  }


  /*
   * Return Stream application configurations for app @name
   */
  def getAppProperties(): Option[Properties] = {
    return getPropertiesFromFile(getConfigPath() + getAppName() + "/" + APP_CONFIG_FILE)
  }

  /*
   * Return producer configurations for app @name
   */
  def getProducerProperties(): Option[Properties] = {
    return getPropertiesFromFile(getConfigPath() + getAppName() + "/" + PROD_CONFIG_FILE)
  }

  /*
   * Return consumer configurations for app @name
   */
  def getConsumerProperties(): Option[Properties] = {
    return getPropertiesFromFile(getConfigPath() + getAppName() + "/" + CONS_CONFIG_FILE)
  }
}
