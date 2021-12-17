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

import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.parser.DSLSQLParser._
import streaming.dsl.template.TemplateMerge
import streaming.dsl.{ScriptSQLExec, ScriptSQLExecListener}
import streaming.log.WowLog
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.tool.Templates2

/**
 * Created by allwefantasy on 12/1/2018.
 */
class TrainAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {

  def evaluate(value: String) = {
    TemplateMerge.merge(value, scriptSQLExecListener.env().toMap)
  }

  def analyze(ctx: SqlContext): TrainStatement = {
    var tableName = ""
    var format = ""
    var path = ""
    var options = Map[String, String]()

    var asTableName = ""
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: TableNameContext =>
          tableName = evaluate(s.getText)
        case s: FormatContext =>
          format = s.getText
        case s: PathContext =>
          path = cleanStr(s.getText)
          path = evaluate(path)
        case s: ExpressionContext =>
          options += (cleanStr(s.qualifiedName().getText) -> evaluate(getStrOrBlockStr(s)))
        case s: BooleanExpressionContext =>
          options += (cleanStr(s.expression().qualifiedName().getText) -> evaluate(getStrOrBlockStr(s.expression())))
        case s: AsTableNameContext =>
          asTableName = evaluate(cleanStr(s.tableName().getText))
        case _ =>
      }
    }
    TrainStatement(currentText(ctx), tableName, format, path, options, asTableName)
  }

  override def parse(ctx: SqlContext): Unit = {
    val TrainStatement(_, tableName, format, _path, _options, asTableName) = analyze(ctx)
    val sqlAlg = MLMapping.findAlg(format)

    var path = _path

    var options = _options.map { case (k, v) =>
      val newV = if(sqlAlg.skipDynamicEvaluation) v else Templates2.dynamicEvaluateExpression(v, ScriptSQLExec.context().execListener.env().toMap)
      (k, newV)
    }

    val owner = options.get("owner")
    val df = scriptSQLExecListener.sparkSession.table(tableName)
    
    if (!sqlAlg.skipPathPrefix) {
      path = withPathPrefix(scriptSQLExecListener.pathPrefix(owner), path)
    }

    if (!sqlAlg.skipOriginalDFName) {
      options = options ++ Map("__dfname__" -> tableName)
    }

    options = options ++ Map("__LINE__" -> ctx.getStart.getLine.toString)

    val firstKeywordInStatement = ctx.getChild(0).getText.toLowerCase

    val isTrain = ETMethod.withName(firstKeywordInStatement) match {
      case ETMethod.PREDICT => false
      case ETMethod.RUN => true
      case ETMethod.TRAIN => true
    }

    if (!skipAuth() && sqlAlg.isInstanceOf[ETAuth]) {
      sqlAlg.asInstanceOf[ETAuth].auth(ETMethod.withName(firstKeywordInStatement), path, options)
    }

    val tempTable = if (asTableName.isEmpty) UUID.randomUUID().toString.replace("-", "") else asTableName

    if (!sqlAlg.skipOriginalDFName) {
      options = options ++ Map("__newdfname__" -> tableName)
    }

    // RUN and TRAIN are the same. TRAIN is normally used for algorithm.
    // RUN is used for other situation.
    val newdf = if (isTrain) {
      sqlAlg.train(df, path, options)
    } else {
      sqlAlg.batchPredict(df, path, options)
    }


    newdf.createOrReplaceTempView(tempTable)
    scriptSQLExecListener.setLastSelectTable(tempTable)
  }

  def skipAuth(): Boolean = {
    scriptSQLExecListener.env()
      .getOrElse("SKIP_AUTH", "true")
      .toBoolean
  }
}

object MLMapping extends Logging with WowLog {
  private val mappingCache = new java.util.concurrent.ConcurrentHashMap[String, SQLAlg]()

  private val mapping = Map[String, String](
    "NaiveBayes" -> "streaming.dsl.mmlib.algs.SQLNaiveBayes",
    "RandomForest" -> "streaming.dsl.mmlib.algs.SQLRandomForest",
    "GBTRegressor" -> "streaming.dsl.mmlib.algs.SQLGBTRegressor",
    "GBTClassifier" -> "streaming.dsl.mmlib.algs.SQLGBTClassifier",
    "LDA" -> "streaming.dsl.mmlib.algs.SQLLDA",
    "KMeans" -> "streaming.dsl.mmlib.algs.SQLKMeans",
    "FPGrowth" -> "streaming.dsl.mmlib.algs.SQLFPGrowth",
    "StringIndex" -> "streaming.dsl.mmlib.algs.SQLStringIndex",
    "GBTs" -> "streaming.dsl.mmlib.algs.SQLGBTs",
    "LSVM" -> "streaming.dsl.mmlib.algs.SQLLSVM",
    "HashTfIdf" -> "streaming.dsl.mmlib.algs.SQLHashTfIdf",
    "LogisticRegression" -> "streaming.dsl.mmlib.algs.SQLLogisticRegression",
    "LinearRegression" -> "streaming.dsl.mmlib.algs.SQLLinearRegressionExt",
    "RowMatrix" -> "streaming.dsl.mmlib.algs.SQLRowMatrix",
    "PageRank" -> "streaming.dsl.mmlib.algs.SQLPageRank",
    "StandardScaler" -> "streaming.dsl.mmlib.algs.SQLStandardScaler",
    "DicOrTableToArray" -> "streaming.dsl.mmlib.algs.SQLDicOrTableToArray",
    "TableToMap" -> "streaming.dsl.mmlib.algs.SQLTableToMap",
    "TokenExtract" -> "streaming.dsl.mmlib.algs.SQLTokenExtract",
    "TokenAnalysis" -> "streaming.dsl.mmlib.algs.SQLTokenAnalysis",
    "TfIdfInPlace" -> "streaming.dsl.mmlib.algs.SQLTfIdfInPlace",
    "Word2VecInPlace" -> "streaming.dsl.mmlib.algs.SQLWord2VecInPlace",
    "RateSampler" -> "streaming.dsl.mmlib.algs.SQLRateSampler",
    "ScalerInPlace" -> "streaming.dsl.mmlib.algs.SQLScalerInPlace",
    "NormalizeInPlace" -> "streaming.dsl.mmlib.algs.SQLNormalizeInPlace",
    "PythonAlg" -> "streaming.dsl.mmlib.algs.SQLPythonAlg",
    "ConfusionMatrix" -> "streaming.dsl.mmlib.algs.SQLConfusionMatrix",
    "Discretizer" -> "streaming.dsl.mmlib.algs.SQLDiscretizer",
    "SendMessage" -> "streaming.dsl.mmlib.algs.SQLSendMessage",
    "JDBC" -> "streaming.dsl.mmlib.algs.SQLJDBC",
    "VecMapInPlace" -> "streaming.dsl.mmlib.algs.SQLVecMapInPlace",
    "Map" -> "streaming.dsl.mmlib.algs.SQLMap",
    "PythonAlgBP" -> "streaming.dsl.mmlib.algs.SQLPythonAlgBatchPrediction",
    "ScalaScriptUDF" -> "streaming.dsl.mmlib.algs.ScriptUDF",
    "ScriptUDF" -> "streaming.dsl.mmlib.algs.ScriptUDF",
    "MapValues" -> "streaming.dsl.mmlib.algs.SQLMapValues",
    "ExternalPythonAlg" -> "streaming.dsl.mmlib.algs.SQLExternalPythonAlg",
    "Kill" -> "streaming.dsl.mmlib.algs.SQLMLSQLJobExt",
    "ALSInPlace" -> "streaming.dsl.mmlib.algs.SQLALSInPlace",
    "AutoIncrementKeyExt" -> "streaming.dsl.mmlib.algs.SQLAutoIncrementKeyExt",
    "CacheExt" -> "streaming.dsl.mmlib.algs.SQLCacheExt",
    "CommunityBasedSimilarityInPlace" -> "streaming.dsl.mmlib.algs.SQLCommunityBasedSimilarityInPlace",
    "CorpusExplainInPlace" -> "streaming.dsl.mmlib.algs.SQLCorpusExplainInPlace",
    "DataSourceExt" -> "streaming.dsl.mmlib.algs.SQLDataSourceExt",
    "DownloadExt" -> "streaming.dsl.mmlib.algs.SQLDownloadExt",
    "FeatureExtractInPlace" -> "streaming.dsl.mmlib.algs.SQLFeatureExtractInPlace",
    "JDBCUpdatExt" -> "streaming.dsl.mmlib.algs.SQLJDBCUpdatExt",
    "ModelExplainInPlace" -> "streaming.dsl.mmlib.algs.SQLModelExplainInPlace",
    "PythonEnvExt" -> "streaming.dsl.mmlib.algs.SQLPythonEnvExt",
    "PythonParallelExt" -> "streaming.dsl.mmlib.algs.SQLPythonParallelExt",
    "RawSimilarInPlace" -> "streaming.dsl.mmlib.algs.SQLRawSimilarInPlace",
    "ReduceFeaturesInPlace" -> "streaming.dsl.mmlib.algs.SQLReduceFeaturesInPlace",
    "RepartitionExt" -> "streaming.dsl.mmlib.algs.SQLRepartitionExt",
    "ShowFunctionsExt" -> "streaming.dsl.mmlib.algs.SQLShowFunctionsExt",
    "TreeBuildExt" -> "streaming.dsl.mmlib.algs.SQLTreeBuildExt",
    "UploadFileToServerExt" -> "streaming.dsl.mmlib.algs.SQLUploadFileToServerExt",
    "WaterMarkInPlace" -> "streaming.dsl.mmlib.algs.SQLWaterMarkInPlace",
    "Word2ArrayInPlace" -> "streaming.dsl.mmlib.algs.SQLWord2ArrayInPlace",
    "AutoML" -> "tech.mlsql.ets.algs.SQLAutoML",
    "ShowTableExt" -> "streaming.dsl.mmlib.algs.SQLShowTableExt",
    "PredictionEva" -> "streaming.dsl.mmlib.algs.SQLPredictionEva"
  )

  /**
   * Get all ET names. Including locally loaded classes and code registered.
   *
   * @return Collection of all ET names. for example: [[scala.collection.immutable.Set("Word2vec", "NaiveBayes")]]
   */
  def getAllETNames: Set[String] = {
    getETMapping.keys.toSet
  }

  /**
   * Get all ETs. Including locally loaded classes and code registered.
   *
   * @return The Map of ET, for example:
   *         [[scala.collection.immutable.Map("Word2vec" -> "streaming.dsl.mmlib.algs.SQLWord2Vec", "Kill" -> "streaming.dsl.mmlib.algs.SQLMLSQLJobExt")]]
   */
  def getETMapping: Map[String, String] = {
    getRegisteredMapping.filter(f =>
      try {
        Some(findET(f._1)).isDefined
      } catch {
        case e: Exception =>
          logError(format("load ET class failed!" + format_throwable(e)))
          false
        case e1: NoClassDefFoundError =>
          logError(format("load ET class failed!" + format_throwable(e1)))
          false
        case _ => false
      })
  }

  /**
   * Get all ET instances for the mapping class. Including locally loaded classes and code registered.
   *
   * @return The Map of ET instances, for example:
   *         [[scala.collection.immutable.Map("Word2vec" -> class[Word2vec], "Kill" -> class[SQLMLSQLJobExt])]]
   */
  def getETInstanceMapping: Map[String, SQLAlg] = {
    getRegisteredMapping.map(f =>
      try {
        f._1 -> findET(f._1)
      } catch {
        case e: Exception =>
          logError(format("load ET class failed!" + format_throwable(e)))
          null
        case e1: NoClassDefFoundError =>
          logError(format("load ET class failed!" + format_throwable(e1)))
          null
        case _ => null
      }).filter(_ != null)
  }

  /**
   * In this method, we combine the `mappings` registered in two different ways, MLMapping and ETRegister.
   * Consistent with [[tech.mlsql.dsl.adaptor.MLMapping#findET(java.lang.String)]] usage.
   *
   * @param name name of algorithm
   * @return algorithm instance
   */
  def findAlg(name: String): SQLAlg = {
    findET(name)
  }

  /**
   * @param name name of ET
   * @return ET instance
   */
  def findET(name: String): SQLAlg = {
    if (mappingCache.contains(name)) {
      return mappingCache.get(name)
    }

    val res: SQLAlg = getRegisteredMapping.get(name.capitalize) match {
      case Some(clzz) =>
        Class.forName(clzz).newInstance().asInstanceOf[SQLAlg]
      case None =>
        logWarning(format("Do not calling unregistered ET! If you are using a custom ET, " +
          "please register it in `ETRegister`."))
        if (!name.contains(".") && (name.endsWith("InPlace") || name.endsWith("Ext"))) {
          Class.forName(s"streaming.dsl.mmlib.algs.SQL${name}").newInstance().asInstanceOf[SQLAlg]
        } else {
          try {
            Class.forName(name).newInstance().asInstanceOf[SQLAlg]
          }
          catch {
            case e: Exception =>
              throw new RuntimeException(s"${name} is not found")
          }
        }
    }
    mappingCache.putIfAbsent(name, res)
    res
  }

  private def getRegisteredMapping: Map[String, String] = {
    MLMapping.mapping ++ ETRegister.getMapping
  }
}

case class TrainStatement(raw: String, inputTableName: String, etName: String, path: String, option: Map[String, String], outputTableName: String)
