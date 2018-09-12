package org.apache.spark.sql.execution

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier

import scala.collection.mutable.ArrayBuffer

/**
  * Created by allwefantasy on 11/9/2018.
  */
object MLSQLAuthParser {
  val parser = new AtomicReference[WowSparkSqlParser]()

  def filterTables(sql: String, session: SparkSession) = {
    val t = ArrayBuffer[TableIdentifier]()
    lazy val parserInstance = new WowSparkSqlParser(session.sqlContext.conf)
    parser.compareAndSet(null, parserInstance)
    parser.get().tables(sql, t)
    t
  }
}
