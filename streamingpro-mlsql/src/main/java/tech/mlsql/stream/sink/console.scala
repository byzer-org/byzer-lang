///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package tech.mlsql.stream.sink
//
///**
//  * 2019-06-03 WilliamZhu(allwefantasy@gmail.com)
//  */
//
//import streaming.dsl.ScriptSQLExec
//import streaming.log.WowLog
//import org.apache.spark.sql._
//import org.apache.spark.sql.execution.streaming.sources.MLSQLConsoleWriter
//import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
//import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, StreamWriteSupport}
//import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister}
//import org.apache.spark.sql.streaming.OutputMode
//import org.apache.spark.sql.types.StructType
//import tech.mlsql.common.utils.log.Logging
//
//import scala.collection.JavaConversions._
//
//
//case class MLSQLConsoleRelation(override val sqlContext: SQLContext, data: DataFrame)
//  extends BaseRelation {
//  override def schema: StructType = data.schema
//}
//
//class MLSQLConsoleSinkProvider extends DataSourceV2
//  with StreamWriteSupport
//  with DataSourceRegister
//  with CreatableRelationProvider with Logging with WowLog {
//
//  val context = ScriptSQLExec.contextGetOrForTest()
//
//
//  override def createStreamWriter(
//                                   queryId: String,
//                                   schema: StructType,
//                                   mode: OutputMode,
//                                   options: DataSourceOptions): StreamWriter = {
//    ScriptSQLExec.setContext(context)
//    val newMaps = options.asMap().toMap ++ Map("LogPrefix" -> s"[owner] [${context.owner}] [groupId] [${context.groupId}]")
//    val newOptions = new DataSourceOptions(newMaps)
//    new MLSQLConsoleWriter(schema, newOptions)
//  }
//
//  def createRelation(
//                      sqlContext: SQLContext,
//                      mode: SaveMode,
//                      parameters: Map[String, String],
//                      data: DataFrame): BaseRelation = {
//    // Number of rows to display, by default 20 rows
//    val numRowsToShow = parameters.get("numRows").map(_.toInt).getOrElse(20)
//
//    // Truncate the displayed data if it is too long, by default it is true
//    val isTruncated = parameters.get("truncate").map(_.toBoolean).getOrElse(true)
//
//    val value = DFVisitor.showString(data, numRowsToShow, 20, isTruncated)
//    value.split("\n").foreach { line =>
//      logInfo(format(line))
//    }
//    MLSQLConsoleRelation(sqlContext, data)
//  }
//
//  def shortName(): String = "mlsql_console"
//
//}
