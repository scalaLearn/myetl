package com.github.db.jdbc

import java.sql.Connection

import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.{DataFrame, XJdbcUtil}

/**
  * @author : ls
  * @version : Created in 下午3:25 2018/6/21
  *
  */
object JdbcDataFrameWriter {
  implicit def dataFrame2JdbcWriter(dataFrame: DataFrame): JdbcDataFrameWriter = JdbcDataFrameWriter(dataFrame)

  def apply(dataFrame: DataFrame): JdbcDataFrameWriter = new JdbcDataFrameWriter(dataFrame)

}

class JdbcDataFrameWriter(dataFrame: DataFrame) extends Serializable {

  private var jdbcSaveExplain: JdbcSaveExplain = _

  def writeJdbc(jdbcSaveExplain: JdbcSaveExplain) = {
    this.jdbcSaveExplain = jdbcSaveExplain
    this
  }

  def save(): Unit = {
    assert(jdbcSaveExplain != null)
    val saveMode = jdbcSaveExplain.saveMode
    val connectionOptions = jdbcSaveExplain.options
    if (checkTable(connectionOptions, saveMode)) XJdbcUtil.saveTable(dataFrame, connectionOptions, saveMode)
  }


  private def checkTable(connectionOptions: JDBCOptions, saveMode: JdbcSaveMode.SaveMode): Boolean = {
    import JdbcSaveMode._

    val conn: Connection = JdbcUtils.createConnectionFactory(connectionOptions).apply()

    try {
      var tableExists = JdbcUtils.tableExists(conn, connectionOptions)
      //table ignore ,exit
      if (saveMode == IgnoreTable && tableExists) {
        println(s"********table ${connectionOptions.table} exists ,mode is ignoreTable,save nothing to it********")
        return false
      }
      //error if table exists
      if (saveMode == ErrorIfExists && tableExists) {
        sys.error(s"********Table ${connectionOptions.table} already exists********")
      }
      //overwrite table ,delete table
      if (saveMode == Overwrite && tableExists) {
        JdbcUtils.dropTable(conn, connectionOptions.table)
        tableExists = false
      }
      if (!tableExists) {
        val schema = JdbcUtils.schemaString(dataFrame, connectionOptions.url)
        val sql = s"CREATE TABLE ${connectionOptions.table} ($schema)"
        conn.prepareStatement(sql).executeUpdate()
      }
      true
    } finally {
      conn.close()
    }
  }
}

final case class JdbcSaveExplain(saveMode: JdbcSaveMode.SaveMode, options: JDBCOptions)

object JdbcSaveMode extends Enumeration {
  type SaveMode = Value
  val ErrorIfExists, IgnoreTable, Append, Overwrite, Upsert, IgnoreRecord = Value

}
