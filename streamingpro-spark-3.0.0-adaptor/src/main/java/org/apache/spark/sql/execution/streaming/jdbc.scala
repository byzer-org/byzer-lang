/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.streaming

/**
  * Created by allwefantasy on 20/8/2018.
  */

import java.sql.{PreparedStatement}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SQLContext}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._


/*
  named parameter is not support yet.
 */
class JDBCSink(_options: Map[String, String]) extends Sink with Logging {

  override def addBatch(batchId: Long, data: DataFrame): Unit = synchronized {
    val options = _options
    val schema = data.schema

    def executeInDriver = {
      val driver = options("driver")
      val url = options("url")
      Class.forName(driver)
      val connection = java.sql.DriverManager.getConnection(url, options("user"), options("password"))
      try {
        // we suppose that there is only one create if
        val statements = options.filter(f =>"""driver\-statement\-[0-9]+""".r.findFirstMatchIn(f._1).nonEmpty).
          map(f => (f._1.split("-").last.toInt, f._2)).toSeq.sortBy(f => f._1).map(f => f._2).map { f =>
          connection.prepareStatement(f)
        }

        statements.map { f =>
          f.execute()
          f
        }.map(_.close())
      } finally {
        if (connection != null)
          connection.close()
      }

    }

    executeInDriver

    val writer = new ForeachWriter[Row] {
      var connection: java.sql.Connection = _
      var statement: java.sql.Statement = _

      var sqlArray: Seq[PreparedStatement] = _

      override def open(partitionId: Long, version: Long): Boolean = {
        val driver = options("driver")
        val url = options("url")
        Class.forName(driver)
        connection = java.sql.DriverManager.getConnection(url, options("user"), options("password"))
        connection.setAutoCommit(false)
        sqlArray = options.filter(f =>"""statement\-[0-9]+""".r.findFirstMatchIn(f._1).nonEmpty).
          filter(f => !f._1.startsWith("driver")).
          map(f => (f._1.split("-").last.toInt, f._2)).toSeq.sortBy(f => f._1).map(f => f._2).map { f =>
          connection.prepareStatement(f)
        }
        if (sqlArray.size == 0) {
          throw new RuntimeException("executor-statement-[number] should be configured")
        }
        true
      }

      def executeUpdate(statement: PreparedStatement, value: Row) = {
        (0 until statement.getParameterMetaData.getParameterCount).foreach { f =>
          val sqlIndex = f + 1
          schema.fields(f).dataType match {
            case StringType => statement.setString(sqlIndex, value.getString(f))
            case IntegerType => statement.setInt(sqlIndex, value.getInt(f))
            case LongType => statement.setLong(sqlIndex, value.getLong(f))
            case DoubleType => statement.setDouble(sqlIndex, value.getDouble(f))
            case NullType => statement.setString(sqlIndex, null)
            case BooleanType => statement.setBoolean(sqlIndex, value.getBoolean(f))
            case DateType => statement.setDate(sqlIndex, value.getDate(f))
            case _ => throw new RuntimeException("JDBC is not support this type")
          }

        }
        statement.addBatch()
        true
      }

      //statement-0 sequence
      override def process(value: Row): Unit = {
        sqlArray.map { st =>
          executeUpdate(st, value)
        }.size
      }

      override def close(errorOrNull: Throwable): Unit = {
        if (connection != null) {
          try {
            sqlArray.map(_.executeBatch()).size
            connection.commit()
          } catch {
            case e: Exception =>
              e.printStackTrace()
              connection.rollback()
          } finally {
            sqlArray.map(_.close())
            connection.close()
          }

        }

      }
    }

    val rowEncoder = RowEncoder.apply(schema).resolveAndBind()

    data.queryExecution.toRdd.foreachPartition { iter =>
      if (writer.open(TaskContext.getPartitionId(), batchId)) {
        try {
          while (iter.hasNext) {
            writer.process(rowEncoder.fromRow(iter.next()))
          }
        } catch {
          case e: Throwable =>
            writer.close(e)
            throw e
        }
        writer.close(null)
      } else {
        writer.close(null)
      }
    }
  }
}

class JDBCSinkProvider extends StreamSinkProvider with DataSourceRegister {
  def createSink(
                  sqlContext: SQLContext,
                  parameters: Map[String, String],
                  partitionColumns: Seq[String],
                  outputMode: OutputMode): Sink = {
    new JDBCSink(parameters)
  }

  def shortName(): String = "jdbc"
}
