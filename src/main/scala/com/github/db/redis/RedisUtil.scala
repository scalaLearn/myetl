package com.github.db.redis

import com.github.util.ConfigUtil
import com.redislabs.provider.redis.RedisContext
import com.redislabs.provider.redis.rdd.RedisKeysRDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author : ls
  * @version : Created in 下午4:56 2018/6/21
  *
  */
object RedisUtil {

  import net.ceedubs.ficus.Ficus._
  import net.ceedubs.ficus.readers.ArbitraryTypeReader._

  private lazy val config = ConfigUtil.readClassPathConfig[RedisConfig]("redis", "config")

  implicit class RedisConfigBuild(conf: SparkConf) {

    def buildRedis: SparkConf = {

      conf.set("redis.host", config.host)
        .set("redis.auth", config.auth.getOrElse(""))
        .set("redis.port", config.port)
        .set("redis.db", config.db)
    }

    def getInitHost: (String, Int) = {
      (config.host, config.port.toInt)
    }
  }

  implicit class RedisReadRdd(session: SparkSession) {

    def keyRDD(key: String, partitionNum: Int = 3): RedisKeysRDD = {
      val redisContext = new RedisContext(session.sparkContext)
      redisContext.fromRedisKeyPattern(key, partitionNum)
    }
  }

}

case class RedisConfig(host: String, port: String = "6379", db: String = "1", auth: Option[String] = None)
