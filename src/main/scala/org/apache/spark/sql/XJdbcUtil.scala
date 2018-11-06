package org.apache.spark.sql

import java.sql.{Connection, PreparedStatement}

import com.github.db.jdbc.JdbcSaveMode
import com.github.db.jdbc.JdbcSaveMode._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils._
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._

import scala.util.control.NonFatal

/**
  * @author : ls
  * @version : Created in 下午3:19 2018/6/21
  *
  */
object XJdbcUtil extends Logging {

  private type JDBCValueSetter = (PreparedStatement, Row, Int, Int) => Unit

  private def insertStatement(conn: Connection, table: String, rddSchema: StructType,
                              dialect: JdbcDialect, saveMode: JdbcSaveMode.SaveMode): PreparedStatement = {
    val columnNames = rddSchema.fields.map(x => dialect.quoteIdentifier(x.name))
    val columns = columnNames.mkString(",")
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")

    val driver = conn.getClientInfo.getProperty("ApplicationName")

    val sql = if (driver.contains("PostgreSQL")) {
      saveMode match {
        case Upsert =>
          val duplicateSetting = columnNames.map(name => s"$name=?").mkString(",")

          val key = columnNames.take(1).apply(0).replaceAll("\"", "")

          /**
            * INSERT INTO the_table (id, column_1, column_2)
            * VALUES (1, 'A', 'X'), (2, 'B', 'Y'), (3, 'C', 'Z')
            * ON CONFLICT (id) DO UPDATE
            * SET column_1 = excluded.column_1,
            * column_2 = excluded.column_2;
            */
          s"INSERT INTO $table ($columns) VALUES ($placeholders) ON CONFLICT($key) DO UPDATE SET $duplicateSetting ;"
        //          s"INSERT INTO $table ($columns) VALUES ($placeholders) ON DUPLICATE KEY UPDATE $duplicateSetting"
        case Append | Overwrite =>
          s"INSERT INTO $table ($columns) VALUES ($placeholders)"
        case IgnoreRecord =>
          s"INSERT IGNORE INTO $table ($columns) VALUES ($placeholders)"
        case _ => throw new IllegalArgumentException(s"$saveMode is illegal")
      }
    } else {
      saveMode match {
        case Upsert =>
          val duplicateSetting = columnNames.map(name => s"$name=?").mkString(",")
          s"INSERT INTO $table ($columns) VALUES ($placeholders) ON DUPLICATE KEY UPDATE $duplicateSetting"
        case Append | Overwrite =>
          s"INSERT INTO $table ($columns) VALUES ($placeholders)"
        case IgnoreRecord =>
          s"INSERT IGNORE INTO $table ($columns) VALUES ($placeholders)"
        case _ => throw new IllegalArgumentException(s"$saveMode is illegal")
      }
    }

    //    val sql = saveMode match {
    //
    //    }
    conn.prepareStatement(sql)
  }

  private def makeSetter(conn: Connection,
                         dialect: JdbcDialect,
                         dataType: DataType): JDBCValueSetter = dataType match {
    case IntegerType =>
      (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) =>
        stmt.setInt(pos + 1, row.getInt(pos - offset))

    case LongType =>
      (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) =>
        stmt.setLong(pos + 1, row.getLong(pos - offset))

    case DoubleType =>
      (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) =>
        stmt.setDouble(pos + 1, row.getDouble(pos - offset))

    case FloatType =>
      (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) =>
        stmt.setFloat(pos + 1, row.getFloat(pos - offset))

    case ShortType =>
      (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) =>
        stmt.setInt(pos + 1, row.getShort(pos - offset))

    case ByteType =>
      (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) =>
        stmt.setInt(pos + 1, row.getByte(pos - offset))

    case BooleanType =>
      (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) =>
        stmt.setBoolean(pos + 1, row.getBoolean(pos - offset))

    case StringType =>
      (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) =>
        stmt.setString(pos + 1, row.getString(pos - offset))

    case BinaryType =>
      (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) =>
        stmt.setBytes(pos + 1, row.getAs[Array[Byte]](pos - offset))

    case TimestampType =>
      (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) =>
        stmt.setTimestamp(pos + 1, row.getAs[java.sql.Timestamp](pos - offset))

    case DateType =>
      (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) =>
        stmt.setDate(pos + 1, row.getAs[java.sql.Date](pos - offset))

    case t: DecimalType =>
      (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) =>
        stmt.setBigDecimal(pos + 1, row.getDecimal(pos - offset))

    case ArrayType(et, _) =>
      // remove type length parameters from end of type name
      val typeName = getJdbcType(et, dialect).databaseTypeDefinition
        .toLowerCase.split("\\(")(0)
      (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) =>
        val array = conn.createArrayOf(
          typeName,
          row.getSeq[AnyRef](pos - offset).toArray)
        stmt.setArray(pos + 1, array)

    case _ =>
      (_: PreparedStatement, _: Row, pos: Int, offset: Int) =>
        throw new IllegalArgumentException(
          s"Can't translate non-null value for field $pos")
  }

  private def getSetter(fields: Array[StructField], connection: Connection,
                        dialect: JdbcDialect, isUpdateMode: Boolean): Array[JDBCValueSetter] = {
    val setter = fields.map(_.dataType).map(makeSetter(connection, dialect, _))
    if (isUpdateMode) Array.fill(2)(setter).flatten else setter
  }

  private def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
    dialect.getJDBCType(dt).orElse(getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.simpleString}"))
  }

  def saveTable(df: DataFrame,
                options: JDBCOptions,
                saveMode: JdbcSaveMode.SaveMode
               ) {

    val dialect = JdbcDialects.get(options.url)
    val nullTypes: Array[Int] = df.schema.fields.map { field =>
      getJdbcType(field.dataType, dialect).jdbcNullType
    }
    val rddSchema = df.schema
    val getConnection: () => Connection = createConnectionFactory(options)
    val batchSize = options.batchSize.toInt
    df.foreachPartition { iterator =>
      savePartition(getConnection, options.table, iterator, rddSchema, nullTypes, batchSize, dialect, 0, saveMode)
    }
  }

  private def savePartition(getConnection: () => Connection,
                            table: String,
                            iterator: Iterator[Row],
                            rddSchema: StructType,
                            nullTypes: Array[Int],
                            batchSize: Int,
                            dialect: JdbcDialect,
                            isolationLevel: Int,
                            saveMode: JdbcSaveMode.SaveMode): Iterator[Byte] = {

    require(batchSize >= 1, s"Invalid value The minimum batchSize value is 1.")

    val conn = getConnection()
    var committed = false
    val isUpdateMode = saveMode == Upsert //check is UpdateMode
    var finalIsolationLevel = Connection.TRANSACTION_NONE
    if (isolationLevel != Connection.TRANSACTION_NONE) {
      try {
        val metadata = conn.getMetaData
        if (metadata.supportsTransactions()) {
          // Update to at least use the default isolation, if any transaction level
          // has been chosen and transactions are supported
          val defaultIsolation = metadata.getDefaultTransactionIsolation
          finalIsolationLevel = defaultIsolation
          if (metadata.supportsTransactionIsolationLevel(isolationLevel)) {
            // Finally update to actually requested level if possible
            finalIsolationLevel = isolationLevel
          } else {
            logWarning(s"Requested isolation level $isolationLevel is not supported; " +
              s"falling back to default isolation level $defaultIsolation")
          }
        } else {
          logWarning(s"Requested isolation level $isolationLevel, but transactions are unsupported")
        }
      } catch {
        case NonFatal(e) => logError("Exception while detecting transaction support", e)
      }
    }
    val supportsTransactions = finalIsolationLevel != Connection.TRANSACTION_NONE

    try {
      if (supportsTransactions) {
        conn.setAutoCommit(false) // Everything in the same db transaction.
        conn.setTransactionIsolation(finalIsolationLevel)
      }
      val length = rddSchema.fields.length
      val numFields = if (isUpdateMode) length * 2 else length // real num Field length
      val stmt = insertStatement(conn, table, rddSchema, dialect, saveMode)
      val setters: Array[JDBCValueSetter] = getSetter(rddSchema.fields, conn, dialect, isUpdateMode) //call method getSetter

      try {
        var rowCount = 0
        while (iterator.hasNext) {
          val row = iterator.next()
          var i = 0
          val midField = numFields / 2
          while (i < numFields) {
            if (isUpdateMode) {
              i < midField match {
                // check midField > i ,if midFiled >i ,rowIndex is setterIndex - (setterIndex/2) + 1
                case true =>
                  if (row.isNullAt(i)) {
                    stmt.setNull(i + 1, nullTypes(i))
                  } else {
                    setters(i).apply(stmt, row, i, 0)
                  }
                case false =>
                  if (row.isNullAt(i - midField)) {
                    stmt.setNull(i + 1, nullTypes(i - midField))
                  } else {
                    setters(i).apply(stmt, row, i, midField)
                  }
              }
            }
            else {
              if (row.isNullAt(i)) {
                stmt.setNull(i + 1, nullTypes(i))
              } else {
                setters(i).apply(stmt, row, i, 0)
              }
            }

            i = i + 1
          }
          stmt.addBatch()
          rowCount += 1
          if (rowCount % batchSize == 0) {
            stmt.executeBatch()
            rowCount = 0
          }
        }
        if (rowCount > 0) {
          stmt.executeBatch()
        }
      } finally {
        stmt.close()
      }
      if (supportsTransactions) {
        conn.commit()
      }
      committed = true
    } catch {
      case e: Exception => logError("jdbc occur some exceptions", e)
    } finally {
      if (!committed) {
        // The stage must fail.  We got here through an exception path, so
        // let the exception through unless rollback() or close() want to
        // tell the user about another problem.
        if (supportsTransactions) {
          conn.rollback()
        }
        conn.close()
      } else {
        // The stage must succeed.  We cannot propagate any exception close() might throw.
        try {
          conn.close()
        } catch {
          case e: Exception => logError("Transaction succeeded, but closing failed", e)
        }
      }
    }
    Array[Byte]().iterator
  }
}
