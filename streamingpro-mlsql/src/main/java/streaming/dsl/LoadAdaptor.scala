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

package streaming.dsl

import net.sf.json.JSONObject
import org.apache.spark.sql.{DataFrame, DataFrameReader, functions => F}
import streaming.core.datasource.{DataSourceConfig, DataSourceRegistry}
import streaming.dsl.load.batch.{AutoWorkflowSelfExplain, MLSQLAPIExplain, MLSQLConfExplain, ModelSelfExplain}
import streaming.dsl.parser.DSLSQLParser._
import streaming.dsl.template.TemplateMerge
import streaming.source.parser.{SourceParser, SourceSchema}

/**
  * Created by allwefantasy on 27/8/2017.
  */
class LoadAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {

  def evaluate(value: String) = {
    TemplateMerge.merge(value, scriptSQLExecListener.env().toMap)
  }

  override def parse(ctx: SqlContext): Unit = {
    var table: DataFrame = null
    var format = ""
    var option = Map[String, String]()
    var path = ""
    var tableName = ""
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: FormatContext =>
          format = s.getText
        case s: ExpressionContext =>
          option += (cleanStr(s.qualifiedName().getText) -> evaluate(getStrOrBlockStr(s)))
        case s: BooleanExpressionContext =>
          option += (cleanStr(s.expression().qualifiedName().getText) -> evaluate(getStrOrBlockStr(s.expression())))
        case s: PathContext =>
          path = s.getText

        case s: TableNameContext =>
          tableName = evaluate(s.getText)
        case _ =>
      }
    }


    if (format.startsWith("kafka") || format.startsWith("mockStream")) {
      scriptSQLExecListener.addEnv("stream", "true")
      new StreamLoadAdaptor(scriptSQLExecListener, option, path, tableName, format).parse
    } else {
      new BatchLoadAdaptor(scriptSQLExecListener, option, path, tableName, format).parse
    }
    scriptSQLExecListener.setLastSelectTable(tableName)

  }
}

class BatchLoadAdaptor(scriptSQLExecListener: ScriptSQLExecListener,
                       option: Map[String, String],
                       var path: String,
                       tableName: String,
                       format: String
                      ) extends DslTool {
  def parse = {
    var table: DataFrame = null
    val sparkSession = scriptSQLExecListener.sparkSession
    val reader = scriptSQLExecListener.sparkSession.read
    reader.options(option)
    path = TemplateMerge.merge(path, scriptSQLExecListener.env().toMap)
    val resourceOwner = option.get("owner")


    DataSourceRegistry.fetch(format, option).map { datasource =>
      def emptyDataFrame = {
        import sparkSession.implicits._
        Seq.empty[String].toDF("name")
      }

      table = datasource.asInstanceOf[ {def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame}].
        load(reader, DataSourceConfig(cleanStr(path), option, Option(emptyDataFrame)))
    }.getOrElse {
      format match {
        case "crawlersql" =>
          table = reader.option("path", cleanStr(path)).format("org.apache.spark.sql.execution.datasources.crawlersql").load()
        case "image" =>
          val resourcePath = resourceRealPath(scriptSQLExecListener, resourceOwner, path)
          table = reader.option("path", resourcePath).format("streaming.dsl.mmlib.algs.processing.image").load()
        case "jsonStr" =>
          val items = cleanBlockStr(scriptSQLExecListener.env()(cleanStr(path))).split("\n")
          import sparkSession.implicits._
          table = reader.json(sparkSession.createDataset[String](items))
        case "script" =>
          val items = List(cleanBlockStr(scriptSQLExecListener.env()(cleanStr(path)))).map { f =>
            val obj = new JSONObject()
            obj.put("content", f)
            obj.toString()
          }
          import sparkSession.implicits._
          table = reader.json(sparkSession.createDataset[String](items))
        case "hive" =>
          table = reader.table(cleanStr(path))
        case "text" =>
          val resourcePath = resourceRealPath(scriptSQLExecListener, resourceOwner, path)
          table = reader.text(resourcePath.split(","): _*)
        case "xml" =>
          val resourcePath = resourceRealPath(scriptSQLExecListener, resourceOwner, path)
          table = reader.option("path", resourcePath).format("com.databricks.spark.xml").load()
        case "mlsqlAPI" =>
          table = new MLSQLAPIExplain(sparkSession).explain
        case "mlsqlConf" =>
          table = new MLSQLConfExplain(sparkSession).explain
        case _ =>

          // calculate resource real absolute path
          val resourcePath = resourceRealPath(scriptSQLExecListener, resourceOwner, path)

          table = ModelSelfExplain(format, cleanStr(path), option, sparkSession).isMatch.thenDo.orElse(() => {

            AutoWorkflowSelfExplain(format, cleanStr(path), option, sparkSession).isMatch.thenDo().orElse(() => {
              reader.format(format).load(resourcePath)
            }).get()

          }).get
      }
    }


    table.createOrReplaceTempView(tableName)
  }
}

class StreamLoadAdaptor(scriptSQLExecListener: ScriptSQLExecListener,
                        option: Map[String, String],
                        var path: String,
                        tableName: String,
                        format: String
                       ) extends DslTool {

  def withWaterMark(table: DataFrame, option: Map[String, String]) = {
    if (option.contains("eventTimeCol")) {
      table.withWatermark(option("eventTimeCol"), option("delayThreshold"))
    } else {
      table
    }

  }

  def parse = {
    var table: DataFrame = null
    val reader = scriptSQLExecListener.sparkSession.readStream
    val cPath = cleanStr(path)
    format match {
      case "kafka" | "socket" =>
        if (!cPath.isEmpty) {
          reader.option("subscribe", cPath)
        }
        table = reader.options(option).format(format).load()
      case "kafka8" | "kafka9" =>
        val format = "com.hortonworks.spark.sql.kafka08"
        /*
           kafka.bootstrap.servers
           kafka.metadata.broker
           startingoffset smallest
         */
        if (!cPath.isEmpty) {
          reader.option("topics", cPath)
        }
        table = reader.format(format).options(option).load()
      case "mockStream" =>
        val format = "org.apache.spark.sql.execution.streaming.mock.MockStreamSourceProvider"
        table = reader.format(format).options(option + ("path" -> cleanStr(path))).load()
      case _ =>
    }
    table = withWaterMark(table, option)

    if (option.contains("valueSchema") && option.contains("valueFormat")) {
      val kafkaFields = List("key", "partition", "offset", "timestamp", "timestampType", "topic")
      val keepOriginalValue = if (option.getOrElse("keepValue", "false").toBoolean) List("value") else List()
      val sourceSchema = new SourceSchema(option("valueSchema"))
      val sourceParserInstance = SourceParser.getSourceParser(option("valueFormat"))

      table = table.withColumn("kafkaValue", F.struct(
        (kafkaFields ++ keepOriginalValue).map(F.col(_)): _*
      )).selectExpr("CAST(value AS STRING) as tmpValue", "kafkaValue")
        .select(sourceParserInstance.parse(F.col("tmpValue"), sourceSchema = sourceSchema, Map()).as("data"), F.col("kafkaValue"))
        .select("data.*", "kafkaValue")
    }

    path = TemplateMerge.merge(path, scriptSQLExecListener.env().toMap)
    table.createOrReplaceTempView(tableName)
  }
}
