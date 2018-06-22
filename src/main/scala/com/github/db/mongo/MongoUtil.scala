package com.github.db.mongo

import com.github.util.ConfigUtil
import com.mongodb._
import com.mongodb.client.MongoCollection
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.sql._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.bson.Document
import scala.collection.JavaConversions._
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._

/**
  * @author : ls
  * @version : Created in 下午3:50 2018/6/21
  *
  */
object MongoUtil {

  private lazy val config = ConfigUtil.readClassPathConfig[MongoConfig]("mongo", "mongo")

  /*  private lazy val uri = config.user match {
      case None => s"mongodb://${config.host}:${config.port}/${config.loginDb}"
      case Some(user) => s"mongodb://${config.user getOrElse ""}:${config.password getOrElse ""}@${config.host}:${config.port}/${config.loginDb}"
    }
    private lazy val mongoClientURI = new MongoClientURI(uri)
    private lazy val mongoClient = new MongoClient(mongoClientURI).getDatabase(config.db)*/

  private lazy val uri = config.user match {
    case None => s"mongodb://${config.host.split(",").map(x => s"${x}:${config.port}").mkString(",")}";
    case Some(user) => s"mongodb://${config.user getOrElse ""}:${config.password getOrElse ""}@${config.host.split(",").map(x => s"${x}:${config.port}").mkString(",")}"
  }


  private lazy val sa = config.host.split(",").map(x => new ServerAddress(x, config.port.toInt)).toSeq
  private lazy val client = new MongoClient(sa)

  private lazy val mongoClient = {
    client.setReadPreference(ReadPreference.primary())
    client.getDatabase(config.db)
  }

  def C(collection: String): MongoCollection[Document] = {
    val doc = mongoClient.getCollection(collection)
    doc
  }

  def createOrDefaultShardIndex(collection: String, key: String = "_id", db: String = config.db) = {
    try {
      val col = C(collection)
      if (col.listIndexes.toSeq.find(_.toJson.contains("hashed")).isEmpty) {
        col.createIndex(new BasicDBObject(key, "hashed"))
        val cmd = new BasicDBObject("shardCollection", s"${config.db}.$collection").
          append("key", new BasicDBObject(key, "hashed"))
        client.getDatabase("admin").runCommand(cmd)
      }
    } catch {
      case e: Exception => println(s"you should check your shard index"); throw e
    } /*finally {
      client.close()
    }*/
  }

  def createOrDefault2dIndex(collection: String, location: String = "location") = {
    try {
      val col = C(collection)
      if (col.listIndexes.toSeq.find(_.toJson.contains("location_2d")).isEmpty)
        col.createIndex(new BasicDBObject("location", "2d"))
    } catch {
      case e: Exception => println(s"you should check your 2d index"); throw e
    } /* finally {
      client.close()
    }*/
  }

  def createOrDefault2dSphereIndex(collection: String, location: String = "location") = {
    try {
      val col = C(collection)
      if (col.listIndexes.toSeq.find(_.toJson.contains("location_2dsphere")).isEmpty)
        col.createIndex(new BasicDBObject("location", "2dsphere"))
    } catch {
      case e: Exception => println(s"you should check your 2dsphere index"); throw e
    } /* finally {
      client.close()
    }*/
  }

  implicit class SqlContextReadMongo(@transient sqlContext: SQLContext) {


    def mongoDF(collection: String, db: String = config.db, shardKey: String = "_id"): DataFrame = {
      val conf = sqlContext.sparkContext.getConf
        .set("spark.mongodb.keep_alive_ms", "15000")
        .set("spark.mongodb.input.uri", uri)
        .set("spark.mongodb.input.database", db)
        .set("spark.mongodb.input.collection", collection)
        //todo https://docs.mongodb.com/manual/reference/read-preference/#replica-set-read-preference-modes
        //todo https://docs.mongodb.com/manual/reference/read-concern/
        //todo https://docs.mongodb.com/manual/core/read-isolation-consistency-recency/
        //需要 使用WiredTiger引擎 并开启  --enableMajorityReadConcern 保证强一致性
        //        .set("spark.mongodb.input.readConcern.level", "majority")
        .set("spark.mongodb.input.readPreference.name", "primary")
        .set("spark.mongodb.input.partitioner", "MongoSamplePartitioner")
        .set("spark.mongodb.input.partitionerOptions.partitionSizeMB", "1024")
      //        .set("spark.mongodb.input.partitioner", "MongoShardedPartitioner")
      //        .set("spark.mongodb.input.partitionerOptions.shardkey", shardKey)
      //        .set("spark.mongodb.input.partitioner", "MongoPaginateByCountPartitioner")
      //        .set("spark.mongodb.input.partitioner.partitionerOptions.numberOfPartitions", "6")
      val readConfig = ReadConfig(conf)
      sqlContext.read.mongo(readConfig)
    }


    def fetchCrawlCouponDF: DataFrame = {
      sqlContext.mongoDF(config.couponTable)
    }

    def fetchCrawlShopDF: DataFrame = {
      sqlContext.mongoDF(config.shopTable)
    }

  }

  implicit class DataFrame2Mongo(df: DataFrame) {


    def save2Mongo(collection: String) {
      val conf = df.sqlContext.sparkContext.getConf
        .set("spark.mongodb.keep_alive_ms", "15000")
        .set("spark.mongodb.output.uri", uri)
        .set("spark.mongodb.output.database", config.db)
        .set("spark.mongodb.output.collection", collection)

      val writeConfig = WriteConfig(conf)
      df.write.mode(SaveMode.Append).mongo(writeConfig)
    }

  }

}

case class MongoConfig(host: String, port: String = "30000", db: String,
                       couponTable: String = "BankCouponInfo", shopTable: String = "MainShop5",
                       user: Option[String] = None, password: Option[String] = None)