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

import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, functions => F}
import streaming.core.datasource._
import streaming.dsl.auth.{LoadAuth, TableAuthResult, TableType}
import streaming.dsl.load.batch.ModelSelfExplain
import streaming.dsl.parser.DSLSQLParser._
import streaming.dsl.template.TemplateMerge
import streaming.dsl.{MLSQLExecuteContext, ScriptSQLExec, ScriptSQLExecListener}
import streaming.source.parser.impl.JsonSourceParser
import streaming.source.parser.{SourceParser, SourceSchema}
import tech.mlsql.MLSQLEnvKey
import tech.mlsql.dsl.auth.DatasourceAuth
import tech.mlsql.runtime.AppRuntimeStore
import tech.mlsql.sql.MLSQLSparkConf
import tech.mlsql.tool.Templates2

/**
 * Created by allwefantasy on 27/8/2017.
 */
class LoadAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {

  def evaluate(value: String) = {
    TemplateMerge.merge(value, scriptSQLExecListener.env().toMap)
  }

  def analyze(ctx: SqlContext): LoadStatement = {
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
    LoadStatement(currentText(ctx), format, path, option, tableName)
  }

  override def parse(ctx: SqlContext): Unit = {
    val LoadStatement(_, format, path, option, tableName) = analyze(ctx)

    def isStream = {
      scriptSQLExecListener.env().contains("streamName")
    }

    if (isStream) {
      scriptSQLExecListener.addEnv("stream", "true")
    }
    new LoadProcessing(scriptSQLExecListener, option, path, tableName, format).parse

    scriptSQLExecListener.setLastSelectTable(tableName)

  }
}

case class LoadStatement(raw: String, format: String, path: String, option: Map[String, String] = Map[String, String](), tableName: String)

class LoadProcessing(scriptSQLExecListener: ScriptSQLExecListener,
                     _option: Map[String, String],
                     var _path: String,
                     tableName: String,
                     format: String
                    ) extends DslTool {
  def parse = {
    var table: DataFrame = null
    val sparkSession = scriptSQLExecListener.sparkSession
    var option = _option
    val tempDS = DataSourceRegistry.fetch(format, option)

    if (tempDS.isDefined) {
      // DataSource who is not MLSQLSourceConfig or if it's MLSQLSourceConfig then  skipDynamicEvaluation is false
      // should evaluate the v with dynamic expression
      if (tempDS.isInstanceOf[MLSQLSourceConfig] && !tempDS.asInstanceOf[MLSQLSourceConfig].skipDynamicEvaluation) {
        option = _option.map { case (k, v) =>
          val newV = Templates2.dynamicEvaluateExpression(v, ScriptSQLExec.context().execListener.env().toMap)
          (k, newV)
        }
      }
    }

    val reader = scriptSQLExecListener.sparkSession.read
    val tempPath = TemplateMerge.merge(_path, scriptSQLExecListener.env().toMap)

    def emptyDataFrame = {
      import sparkSession.implicits._
      Seq.empty[String].toDF("name")
    }

    val dsConf = optionsRewrite(
      AppRuntimeStore.LOAD_BEFORE_CONFIG_KEY,
      DataSourceConfig(cleanStr(tempPath), option, Option(emptyDataFrame)),
      format,
      ScriptSQLExec.context())

    val path = dsConf.path

    reader.options(dsConf.config)
    var sourceInfo: Option[SourceInfo] = None


    if (scriptSQLExecListener.authProcessListner.isDefined) {
      val auth = new LoadAuth(scriptSQLExecListener.authProcessListner.get)
      val tables = auth.getAuthTables(format, path, option)
      val context = ScriptSQLExec.contextGetOrForTest()
      context.execListener.getTableAuth match {
        case Some(tableAuth) =>
          tableAuth.auth(tables.get)
        case None => List(TableAuthResult(true, ""))
      }
    }


    DataSourceRegistry.fetch(format, option).map { datasource =>
      val ds = datasource.asInstanceOf[ {def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame}]
      table = ds.load(reader, dsConf)

      // extract source info if the datasource is  MLSQLSourceInfo
      if (datasource.isInstanceOf[MLSQLSourceInfo]) {
        val authConf = DataAuthConfig(dsConf.path, dsConf.config)
        sourceInfo = Option(sourceInfoRewrite(
          AppRuntimeStore.LOAD_BEFORE_CONFIG_KEY,
          datasource.asInstanceOf[MLSQLSourceInfo].sourceInfo(authConf),
          format,
          ScriptSQLExec.context()))
      }
      if (datasource.isInstanceOf[DatasourceAuth]) {
        datasource.asInstanceOf[DatasourceAuth].auth(dsConf.path, dsConf.config)
      }
      // return the load table
      table
    }.getOrElse {
      // path could be:
      // 1) fileSystem path; code example: load  modelExplain.`/tmp/model` where alg="RandomForest" as output;
      // 2) ET name; code example: load modelExample.`JsonExpandExt` AS output_1;  load modelParams.`JsonExpandExt` as output;
      // For FileSystem path, pass the real path to ModelSelfExplain; for ET name pass original path
      val resourcePath = resourceRealPath(scriptSQLExecListener, option.get("owner"), path)
      val fsPathOrETName = format match {
        case "modelExplain" => resourcePath
        case _ => cleanStr(path)
      }
      table = ModelSelfExplain(format, fsPathOrETName, option, sparkSession).isMatch.thenDo.orElse(() => {
        reader.format(format).load(resourcePath)
      }).get
    }

    table = customRewrite(AppRuntimeStore.LOAD_BEFORE_KEY, table, dsConf, sourceInfo, ScriptSQLExec.context())
    // In order to control the access of columns, we should rewrite the final sql (convert * to specify column names)
    table = authRewrite(table, dsConf, sourceInfo, ScriptSQLExec.context())
    // finally use the  build-in or third-party plugins to rewrite the table.
    table = customRewrite(AppRuntimeStore.LOAD_AFTER_KEY, table, dsConf, sourceInfo, ScriptSQLExec.context())

    def isStream = {
      scriptSQLExecListener.env().contains("streamName")
    }

    def isStreamSource(name: String) = {
      (TableType.KAFKA.includes ++ TableType.SOCKET.includes ++ List("mockStream")).contains(name)
    }

    if (isStream || isStreamSource(format)) {

      def withWaterMark(table: DataFrame, option: Map[String, String]) = {
        if (option.contains("eventTimeCol")) {
          table.withWatermark(option("eventTimeCol"), option("delayThreshold"))
        } else {
          table
        }

      }

      table = withWaterMark(table, option)

      def deserializeSchema(json: String): StructType = {
        DataType.fromJson(json) match {
          case t: StructType => t
          case _ => throw new RuntimeException(s"Failed parsing StructType: $json")
        }
      }


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

      if (!option.contains("valueSchema") && !option.contains("valueFormat")) {
        val context = ScriptSQLExec.contextGetOrForTest()

        val kafkaSchemaOpt = context.execListener.env().get(MLSQLEnvKey.CONTEXT_KAFKA_SCHEMA)
        kafkaSchemaOpt match {
          case Some(schema) =>

            val kafkaFields = List("key", "partition", "offset", "timestamp", "timestampType", "topic")
            val keepOriginalValue = if (option.getOrElse("keepValue", "false").toBoolean) List("value") else List()
            val sourceSchema = deserializeSchema(schema)
            val sourceParserInstance = new JsonSourceParser()

            table = table.withColumn("kafkaValue", F.struct(
              (kafkaFields ++ keepOriginalValue).map(F.col(_)): _*
            )).selectExpr("CAST(value AS STRING) as tmpValue", "kafkaValue")
              .select(sourceParserInstance.parseRaw(F.col("tmpValue"), sourceSchema, Map()).as("data"), F.col("kafkaValue"))
              .select("data.*", "kafkaValue")
          case None =>
        }
      }


      //path = TemplateMerge.merge(path, scriptSQLExecListener.env().toMap)
    }

    table.createOrReplaceTempView(tableName)
  }

  def authRewrite(df: DataFrame,
                  config: DataSourceConfig,
                  sourceInfo: Option[SourceInfo],
                  context: MLSQLExecuteContext): DataFrame = {
    val rewrite = MLSQLSparkConf.runtimeLoadRewrite

    val implClass = MLSQLSparkConf.runtimeLoadRewriteImpl

    if (rewrite && implClass != "") {
      val instance = Class.forName(implClass)
      instance.newInstance()
        .asInstanceOf[RewritableSource]
        .rewrite(df, config, sourceInfo, context)

    } else {
      df
    }
  }

  def optionsRewrite(orderKey: String,
                     config: DataSourceConfig,
                     format: String,
                     context: MLSQLExecuteContext) = {
    AppRuntimeStore.store.getLoadSave(orderKey) match {
      case Some(item) =>
        item.customClassItems.classNames.map { className =>
          val instance = Class.forName(className).newInstance().asInstanceOf[RewritableSourceConfig]
          instance.rewrite_conf(config, format, context)
        }.headOption.getOrElse(config)
      case None =>
        config
    }
  }

  def sourceInfoRewrite(orderKey: String,
                        sourceInfo: SourceInfo,
                        format: String,
                        context: MLSQLExecuteContext) = {
    AppRuntimeStore.store.getLoadSave(orderKey) match {
      case Some(item) =>
        item.customClassItems.classNames.map { className =>
          val instance = Class.forName(className).newInstance().asInstanceOf[RewritableSourceConfig]
          instance.rewrite_source(sourceInfo, format, context)
        }.headOption.getOrElse(sourceInfo)
      case None =>
        sourceInfo
    }
  }

  def customRewrite(orderKey: String, df: DataFrame,
                    config: DataSourceConfig,
                    sourceInfo: Option[SourceInfo],
                    context: MLSQLExecuteContext) = {
    var newDF = df
    AppRuntimeStore.store.getLoadSave(orderKey) match {
      case Some(item) =>

        item.customClassItems.classNames.foreach { className =>
          val instance = Class.forName(className)
          newDF = instance.newInstance()
            .asInstanceOf[RewritableSource]
            .rewrite(df, config, sourceInfo, context)
        }
      case None =>
    }
    newDF
  }
}


