package com

import com.github.util.ConfigUtil
import org.apache.spark.sql.DataFrame

/**
  * @author : ls
  * @version : Created in 下午3:10 2018/6/21
  *
  */
package object github {

  import net.ceedubs.ficus.Ficus._
  import net.ceedubs.ficus.readers.ArbitraryTypeReader._

  sealed case class SparkCommonConf(shufflePartitions: String)

  private lazy val sparkCommonConf = ConfigUtil.readClassPathConfig[SparkCommonConf]("sparkconf", "conf")

  lazy val spark_shuffle_partitions = sparkCommonConf.shufflePartitions

  implicit class JobFilter(df: DataFrame) {

    import FilterCase._

    def filterDaily(dateTime: Option[String] = None, caseMode: CaseMode): DataFrame = {
      dateTime match {
        case Some(dt) => {
          caseMode match {
            case LowerCase => df.filter(s"date_format(rec_upd_ts,'yyyy-MM-dd')='${dt}'")
            case UpperCase => df.filter(s"date_format(REC_UPD_TS,'yyyy-MM-dd')='${dt}'")
          }
        }
        case None => df
      }
    }

    def filterRest(startTime: String, endTime: String, caseMode: CaseMode): DataFrame = {
      caseMode match {
        case LowerCase => df.filter(s"date_format(rec_upd_ts,'yyyy-MM-ddHH:mm:ss')>='${startTime}' and date_format(rec_upd_ts,'yyyy-MM-ddHH:mm:ss')<='${endTime}'")
        case UpperCase => df.filter(s"date_format(REC_UPD_TS,'yyyy-MM-ddHH:mm:ss')>='${startTime}' and date_format(REC_UPD_TS,'yyyy-MM-ddHH:mm:ss')<='${endTime}'")
      }
    }


    def mongoDaily(date: Option[String] = None): DataFrame = {
      date match {
        case Some(dt) => df.filter(s"date_format(updateAt,'yyyy-MM-dd')='${dt}'")
        case None => df
      }
    }

  }

  object FilterCase extends Enumeration {
    type CaseMode = Value
    val LowerCase, UpperCase = Value
  }

}
