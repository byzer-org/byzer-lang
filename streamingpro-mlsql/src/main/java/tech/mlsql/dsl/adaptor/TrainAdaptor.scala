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

import org.apache.spark.SparkCoreVersion
import streaming.dsl.ScriptSQLExecListener
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.parser.DSLSQLParser._
import streaming.dsl.template.TemplateMerge
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod
import tech.mlsql.ets.register.ETRegister

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
    var path = _path
    var options = _options
    val owner = options.get("owner")
    val df = scriptSQLExecListener.sparkSession.table(tableName)
    val sqlAlg = MLMapping.findAlg(format)
    //2.3.1
    val coreVersion = SparkCoreVersion.version
    if (sqlAlg.coreCompatibility.filter(f => f.coreVersion == coreVersion).size == 0) {
      throw new RuntimeException(s"name: $format class:${sqlAlg.getClass.getName} is not compatible with current core version:$coreVersion")
    }

    if (!sqlAlg.skipPathPrefix) {
      path = withPathPrefix(scriptSQLExecListener.pathPrefix(owner), path)
    }

    if (!sqlAlg.skipOriginalDFName) {
      options = options ++ Map("__dfname__" -> tableName)
    }

    val firstKeywordInStatement = ctx.getChild(0).getText

    val isTrain = ETMethod.withName(firstKeywordInStatement) match {
      case ETMethod.PREDICT => false
      case ETMethod.RUN => true
      case ETMethod.TRAIN => true
    }

    if (!skipAuth() && sqlAlg.isInstanceOf[ETAuth]) {
      sqlAlg.asInstanceOf[ETAuth].auth(ETMethod.withName(firstKeywordInStatement), path, options)
    }

    // RUN and TRAIN are the same. TRAIN is normally used for algorithm.
    // RUN is used for other situation.
    val newdf = if (isTrain) {
      sqlAlg.train(df, path, options)
    } else {
      sqlAlg.batchPredict(df, path, options)
    }

    val tempTable = if (asTableName.isEmpty) UUID.randomUUID().toString.replace("-", "") else asTableName
    newdf.createOrReplaceTempView(tempTable)
    scriptSQLExecListener.setLastSelectTable(tempTable)
  }

  def skipAuth(): Boolean = {
    scriptSQLExecListener.env()
      .getOrElse("SKIP_AUTH", "true")
      .toBoolean
  }
}

object MLMapping {
  val mapping = ETRegister.mapping ++ Map[String, String](
    "Word2vec" -> "streaming.dsl.mmlib.algs.SQLWord2Vec",
    "NaiveBayes" -> "streaming.dsl.mmlib.algs.SQLNaiveBayes",
    "RandomForest" -> "streaming.dsl.mmlib.algs.SQLRandomForest",
    "GBTRegressor" -> "streaming.dsl.mmlib.algs.SQLGBTRegressor",
    "LDA" -> "streaming.dsl.mmlib.algs.SQLLDA",
    "KMeans" -> "streaming.dsl.mmlib.algs.SQLKMeans",
    "FPGrowth" -> "streaming.dsl.mmlib.algs.SQLFPGrowth",
    "StringIndex" -> "streaming.dsl.mmlib.algs.SQLStringIndex",
    "GBTs" -> "streaming.dsl.mmlib.algs.SQLGBTs",
    "LSVM" -> "streaming.dsl.mmlib.algs.SQLLSVM",
    "HashTfIdf" -> "streaming.dsl.mmlib.algs.SQLHashTfIdf",
    "TfIdf" -> "streaming.dsl.mmlib.algs.SQLTfIdf",
    "LogisticRegressor" -> "streaming.dsl.mmlib.algs.SQLLogisticRegression",
    "RowMatrix" -> "streaming.dsl.mmlib.algs.SQLRowMatrix",
    "PageRank" -> "streaming.dsl.mmlib.algs.SQLPageRank",
    "StandardScaler" -> "streaming.dsl.mmlib.algs.SQLStandardScaler",
    "DicOrTableToArray" -> "streaming.dsl.mmlib.algs.SQLDicOrTableToArray",
    "TableToMap" -> "streaming.dsl.mmlib.algs.SQLTableToMap",
    "DL4J" -> "streaming.dsl.mmlib.algs.SQLDL4J",
    "TokenExtract" -> "streaming.dsl.mmlib.algs.SQLTokenExtract",
    "TokenAnalysis" -> "streaming.dsl.mmlib.algs.SQLTokenAnalysis",
    "TfIdfInPlace" -> "streaming.dsl.mmlib.algs.SQLTfIdfInPlace",
    "Word2VecInPlace" -> "streaming.dsl.mmlib.algs.SQLWord2VecInPlace",
    "RateSampler" -> "streaming.dsl.mmlib.algs.SQLRateSampler",
    "ScalerInPlace" -> "streaming.dsl.mmlib.algs.SQLScalerInPlace",
    "NormalizeInPlace" -> "streaming.dsl.mmlib.algs.SQLNormalizeInPlace",
    "PythonAlg" -> "streaming.dsl.mmlib.algs.SQLPythonAlg",
    "ConfusionMatrix" -> "streaming.dsl.mmlib.algs.SQLConfusionMatrix",
    "OpenCVImage" -> "streaming.dsl.mmlib.algs.processing.SQLOpenCVImage",
    "JavaImage" -> "streaming.dsl.mmlib.algs.processing.SQLJavaImage",
    "Discretizer" -> "streaming.dsl.mmlib.algs.SQLDiscretizer",
    "SendMessage" -> "streaming.dsl.mmlib.algs.SQLSendMessage",
    "JDBC" -> "streaming.dsl.mmlib.algs.SQLJDBC",
    "VecMapInPlace" -> "streaming.dsl.mmlib.algs.SQLVecMapInPlace",
    "DTFAlg" -> "streaming.dsl.mmlib.algs.SQLDTFAlg",
    "Map" -> "streaming.dsl.mmlib.algs.SQLMap",
    "PythonAlgBP" -> "streaming.dsl.mmlib.algs.SQLPythonAlgBatchPrediction",
    "ScalaScriptUDF" -> "streaming.dsl.mmlib.algs.ScriptUDF",
    "ScriptUDF" -> "streaming.dsl.mmlib.algs.ScriptUDF",
    "MapValues" -> "streaming.dsl.mmlib.algs.SQLMapValues",
    "ExternalPythonAlg" -> "streaming.dsl.mmlib.algs.SQLExternalPythonAlg",
    "Kill" -> "streaming.dsl.mmlib.algs.SQLMLSQLJobExt"

  )

  def findAlg(name: String) = {
    mapping.get(name.capitalize) match {
      case Some(clzz) =>
        Class.forName(clzz).newInstance().asInstanceOf[SQLAlg]
      case None =>
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
  }
}

case class TrainStatement(raw: String, inputTableName: String, etName: String, path: String, option: Map[String, String], outputTableName: String)
