package com.github.test

import com.github.db.jdbc.{JdbcSaveMode, JdbcUtil}
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * @author : ls
  * @version : Created in 下午3:32 2018/6/21
  *
  */
object Test {

  def main(args: Array[String]): Unit = {


    val sparkSession = SparkSession
      .builder()
      .appName("BizAreaJob--商区任务")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    implicit val sqlContext: SQLContext = sparkSession.sqlContext
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")


    import sqlContext.implicits._


    val insertDF = Seq(User(1, "ls", "lsppp"), User(2, "pp", "pppuu"))
      .toDF("id", "account", "passwd")

    JdbcUtil.saveToPG("my_user", JdbcSaveMode.Upsert)(insertDF)

    //        JdbcUtil.saveToMysql("my_user", JdbcSaveMode.Upsert)(insertDF)
    //
    //    val df = JdbcUtil.mysqlJdbcDF("my_user")

    val df = JdbcUtil.PGJdbcDF("my_user")

    df.printSchema()
    df.show()

    sparkSession.stop()
  }

  case class User(id: Int, name: String, password: String)

}
