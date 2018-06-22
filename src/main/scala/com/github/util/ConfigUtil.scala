package com.github.util

import java.io.File
import java.net.URLDecoder

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader

/**
  * @author : ls
  * @version : Created in 下午3:06 2018/6/21
  *
  */
object ConfigUtil {

  private lazy val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  /**
    *
    * @param path
    * @param root
    * @tparam T
    * @return
    */
  def readFileConfig[T: ValueReader](path: String, root: String = "task") = {
    val myConfigFile = new File(path)
    val fileConfig = ConfigFactory.parseFile(myConfigFile)
    val config: Config = ConfigFactory.load(fileConfig)
    val tasks = config.as[T](root)
    tasks
  }

  /**
    *
    * @param configFileName
    * @param root
    * @tparam T
    * @return
    */
  def readClassPathConfig[T: ValueReader](configFileName: String, root: String = "task") = {
    val config: Config = ConfigFactory.load(configFileName)
    val tasks = config.as[T](root)
    tasks
  }

  /**
    *
    * @param encodeString
    * @tparam T
    * @return
    */

  def readEncodeStringConfig[T: ValueReader](encodeString: String) = {
    val configJson = URLDecoder.decode(encodeString, "UTF-8")
    readStringConfig[T](configJson)
  }

  /**
    *
    * @param string
    * @tparam T
    * @return
    */
  def readStringConfig[T: ValueReader](string: String) = {
    println(s"parameters:\n\r${string}")
    val root = string.split(" ")(0).replaceAll("\n", "")
    val config: Config = ConfigFactory.parseString(string)
    val task = config.as[T](root)
    println(s"parse result:\r\n${(root, task)}")
    (root, task)
  }

  /**
    *
    * @param string
    * @param clazz
    * @tparam T
    * @return
    */
  def readEncodingJson[T](string: String)(implicit clazz: Class[T]) = {
    val configJson = URLDecoder.decode(string, "UTF-8")
    readJson(configJson)
  }

  /**
    *
    * @param configJson
    * @param clazz
    * @tparam T
    * @return
    */
  def readJson[T](configJson: String)(implicit clazz: Class[T]) = {
    objectMapper.readValue(configJson, clazz)
  }

  /**
    *
    * @param m
    * @param clazz
    * @tparam T
    * @return
    */
  def map2Class[T](m: AnyRef)(implicit clazz: Class[T]) = {
    val str = objectMapper.writeValueAsString(m)
    objectMapper.readValue(str, clazz)
  }

}
