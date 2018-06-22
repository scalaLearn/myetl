package com.github.test

import com.github.db.redis.RedisUtil
import org.apache.spark.sql.SparkSession

/**
  * @author : ls
  * @version : Created in 下午5:25 2018/6/21
  *
  */
object RedisTest {

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


    RedisUtil.RedisReadRdd(sparkSession).keyRDD("xxx")
      .foreach(println)

  }


}
