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

package tech.mlsql.dsl.adaptor

import java.util.UUID

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode}
import streaming.core.datasource.{DataSinkConfig, DataSourceRegistry}
import streaming.core.stream.MLSQLStreamManager
import streaming.dsl.parser.DSLSQLParser._
import streaming.dsl.template.TemplateMerge
import streaming.dsl.{ScriptSQLExec, ScriptSQLExecListener}
import tech.mlsql.job.{JobManager, MLSQLJobType}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by allwefantasy on 27/8/2017.
  */
class SaveAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {

  def evaluate(value: String) = {
    TemplateMerge.merge(value, scriptSQLExecListener.env().toMap)
  }

  def analyze(ctx: SqlContext): SaveStatement = {
    var mode = SaveMode.ErrorIfExists
    var format = ""
    var option = Map[String, String]()
    var tableName = ""
    var partitionByCol = ArrayBuffer[String]()
    var path = ""

    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: FormatContext =>
          format = s.getText

        case s: PathContext =>
          path = TemplateMerge.merge(cleanStr(s.getText), scriptSQLExecListener.env().toMap)

        case s: TableNameContext =>
          tableName = evaluate(s.getText)

        case s: OverwriteContext =>
          mode = SaveMode.Overwrite
        case s: AppendContext =>
          mode = SaveMode.Append
        case s: ErrorIfExistsContext =>
          mode = SaveMode.ErrorIfExists
        case s: IgnoreContext =>
          mode = SaveMode.Ignore
        case s: ColContext =>
          partitionByCol += cleanStr(s.identifier().getText)
        case s: ColGroupContext =>
          partitionByCol += cleanStr(s.col().identifier().getText)
        case s: ExpressionContext =>
          option += (cleanStr(s.qualifiedName().getText) -> evaluate(getStrOrBlockStr(s)))
        case s: BooleanExpressionContext =>
          option += (cleanStr(s.expression().qualifiedName().getText) -> evaluate(getStrOrBlockStr(s.expression())))
        case _ =>
      }
    }
    SaveStatement(currentText(ctx), tableName, format, path, option, mode.toString, partitionByCol.toList)
  }

  override def parse(ctx: SqlContext): Unit = {


    val SaveStatement(_, tableName, format, path, option, _mode, partitionByCol) = analyze(ctx)
    val owner = option.get("owner")
    val mode = SaveMode.valueOf(_mode)
    var oldDF: DataFrame = scriptSQLExecListener.sparkSession.table(tableName)

    def isStream = {
      MLSQLStreamManager.isStream
    }

    val spark = oldDF.sparkSession
    import spark.implicits._
    val context = ScriptSQLExec.context()
    var job = JobManager.getJobInfo(context.groupId)


    if (isStream) {
      job = job.copy(jobType = MLSQLJobType.STREAM, jobName = scriptSQLExecListener.env()("streamName"))
      JobManager.addJobManually(job)
    }

    var streamQuery: StreamingQuery = null

    if (option.contains("fileNum")) {
      oldDF = oldDF.repartition(option.getOrElse("fileNum", "").toString.toInt)
    }
    val writer = if (isStream) null else oldDF.write

    val saveRes = DataSourceRegistry.fetch(format, option).map { datasource =>
      val newOption = if (partitionByCol.size > 0) {
        option ++ Map("partitionByCol" -> partitionByCol.mkString(","))
      } else option

      val res = datasource.asInstanceOf[ {def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Any}].save(
        writer,
        // here we should change final_path to path in future
        DataSinkConfig(path, newOption,
          mode, Option(oldDF)))
      res
    }.getOrElse {

      if (isStream) {
        throw new RuntimeException(s"save is not support with ${format}  in stream mode")
      }
      if (partitionByCol.size != 0) {
        writer.partitionBy(partitionByCol: _*)
      }

      writer.mode(mode)

      if (path == "-" || path.isEmpty) {
        writer.format(option.getOrElse("implClass", format)).save()
      } else {
        writer.format(option.getOrElse("implClass", format)).save(resourceRealPath(context.execListener, owner, path))
      }
    }

    if (isStream) {
      streamQuery = saveRes.asInstanceOf[StreamingQuery]
    }

    job = JobManager.getJobInfo(context.groupId)
    if (streamQuery != null) {
      // Todo:Notice that sometimes the MLSQLStreamingQueryListener.onQueryStarted will be executed before this code,
      //  and this may cause some issues. We may try to fix this in future.
      JobManager.removeJobManually(job.groupId)
      val realGroupId = streamQuery.id.toString
      if (!JobManager.getJobInfo.contains(realGroupId)) {
        JobManager.addJobManually(job.copy(groupId = realGroupId))
      }
      job = JobManager.getJobInfo(realGroupId)
      MLSQLStreamManager.addStore(job)
    }

    val tempTable = UUID.randomUUID().toString.replace("-", "")
    val outputTable = spark.createDataset(Seq(job))
    outputTable.createOrReplaceTempView(tempTable)
    scriptSQLExecListener.setLastSelectTable(tempTable)
  }
}

case class SaveStatement(raw: String, inputTableName: String, format: String, path: String, option: Map[String, String] = Map(), mode: String, partitionByCol: List[String])
