package com.github.db.jdbc

import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.github.db.jdbc.JdbcDataFrameWriter._
import com.github.util.ConfigUtil
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * @author : ls
  * @version : Created in 下午3:25 2018/6/21
  *
  */
private[github] trait JdbcConnection {

  def user: String = ???

  def password: String = ???

  def driver: String = ???

  def url: String = ???


  def properties(driver: String): Properties = {
    val properties = new Properties
    properties.put("driver", driver)
    //批量插入的数据大小
    properties.put("batchsize", "10000")
    //    properties.put("batchsize", "10000")
    properties
  }

  implicit class ReadJdbc(@transient sqlContext: SQLContext) {

    def jdbcDF(table: String, numPartitions: Int = 6): DataFrame = {
      val map = Map("url" -> url, "driver" -> driver, "dbtable" -> table, "numPartitions" -> s"$numPartitions")
      sqlContext.read.format("jdbc").options(map).load()
    }

  }

  implicit class DataFrame2Jdbc(df: DataFrame) {
    def save2DB(table: String, numPartitions: Option[Int] = None) = {
      //      val map = Map("url" -> url, "driver" -> driver, "dbtable" -> table, "numPartitions" -> s"$numPartitions")
      numPartitions match {
        case Some(num) => df.coalesce(num).write.mode(SaveMode.Append).jdbc(url, table, properties(driver))
        case None => df.write.mode(SaveMode.Append).jdbc(url, table, properties(driver))
      }

    }

  }

}

case class MysqlConnection(configName: String, rootNode: String) extends JdbcConnection {

  def config = JDBCConfig.readConfig(configName, rootNode)

  require(config.driver.contains("mysql"), "driver name must be mysql")

  override def user = config.user

  override def password = config.password

  override def driver = config.driver

  override def url = config.url
}

private[github] object MysqlConnection {
  def build(configName: String, rootNode: String): MysqlConnection = MysqlConnection(configName, rootNode)
}


case class Db2Connection(configName: String, rootNode: String) extends JdbcConnection {

  def config = JDBCConfig.readConfig(configName, rootNode)

  require(config.driver.contains("db2"), "driver name must be db2")

  override def user = config.user

  override def password = config.password

  override def driver = config.driver

  override def url = config.url
}

private[github] object Db2Connection {
  def build(configName: String, rootNode: String): Db2Connection = Db2Connection(configName, rootNode)

}

object JDBCConfig {

  import net.ceedubs.ficus.readers.ArbitraryTypeReader._
  import net.ceedubs.ficus.Ficus._

  def readConfig(configName: String, rootNode: String): JDBCConfig = {
    ConfigUtil.readClassPathConfig[JDBCConfig](configName, rootNode).build
  }
}


case class JDBCConfig(user: String,
                      password: String,
                      db: String,
                      schema: Option[String] = None,
                      driver: String,
                      host: String,
                      var url: String) {
  def build = appendArgs

  def appendArgs: JDBCConfig = {
    url = s"$url"
      .replace("$host", host)
      .replace("$db", db)
      .replace("$user", user)
      .replace("$password", password)
    schema match {
      case None => this
      case Some(sc) => url = url.replace("$schema", sc); this
    }
  }
}


object JdbcUtil {

  private lazy val numPartitions = 6

  def mysqlJdbcDF(table: String, rootNode: String = "source", numPartitions: Int = numPartitions, configName: String = "mysql2mysql")(implicit sqlContext: SQLContext): DataFrame = {
    val mysql = MysqlConnection.build(configName, rootNode)
    import mysql._
    sqlContext.jdbcDF(table, numPartitions)
  }

  /**
    * spark 原生支持
    *
    * @param table
    * @param numPartitions
    * @param configName
    * @param rootNode
    * @param dataFrame
    */
  def save2Mysql(table: String, numPartitions: Option[Int] = None, configName: String = "mysql2mysql", rootNode: String = "sink")(implicit dataFrame: DataFrame) = {
    val mysql = MysqlConnection.build(configName, rootNode)
    import mysql._
    dataFrame.save2DB(table, numPartitions /* orElse Option(1)*/)
  }

  def getMysqlJdbcConnection(rootNode: String = "sink", configName: String = "mysql2mysql"): Connection = {
    val config = JDBCConfig.readConfig(configName, rootNode)
    Class.forName(config.driver)
    DriverManager.getConnection(config.url, config.user, config.password)
  }

  /**
    * 实现行级别封装
    *
    * @param table
    * @param saveMode
    * @param rootNode
    * @param configName
    * @param dataFrame
    */
  def saveToMysql(table: String, saveMode: JdbcSaveMode.SaveMode,
                  rootNode: String = "sink", configName: String = "mysql2mysql")(implicit dataFrame: DataFrame) = {
    val explain = buildJdbcSaveExplain(table, saveMode, rootNode, configName)
    dataFrame.writeJdbc(explain).save()
  }


  def db2JdbcDF(table: String, numPartitions: Int = numPartitions, configName: String = "db2mysql", rootNode: String = "source")(implicit sqlContext: SQLContext): DataFrame = {
    val db2 = Db2Connection.build(configName, rootNode)
    import db2._
    sqlContext.jdbcDF(table, numPartitions)
  }

  /**
    * spark 原生支持
    *
    * @param table
    * @param numPartitions
    * @param configName
    * @param rootNode
    * @param dataFrame
    */
  def save2DB2(table: String, numPartitions: Option[Int] = None, configName: String = "db2mysql", rootNode: String = "source")(implicit dataFrame: DataFrame) = {
    val db2 = Db2Connection.build(configName, rootNode)
    import db2._
    dataFrame.save2DB(table, numPartitions /* orElse Option(1)*/)
  }


  private def buildJdbcSaveExplain(table: String, saveMode: JdbcSaveMode.SaveMode,
                                   rootNode: String, configName: String): JdbcSaveExplain = {
    val config = JDBCConfig.readConfig(configName, rootNode)

    val parameters: Map[String, String] = Map[String, String]("driver" -> config.driver, "batchsize" -> "10000")

    val options: JDBCOptions = new JDBCOptions(config.url, table, parameters)

    JdbcSaveExplain(saveMode, options)
  }
}
