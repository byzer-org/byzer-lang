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

package streaming.dsl.mmlib.algs

import java.util.ArrayList

import org.apache.spark.ml.linalg.{Matrices, Matrix, Vector}
import org.apache.spark.ps.cluster.Message
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.param.{BaseParams, SQLPythonAlgParams}
import streaming.dsl.mmlib.algs.python._
import streaming.log.WowLog
import tech.mlsql.common.utils.hdfs.HDFSOperator
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.ets.alg.BaseAlg

import scala.collection.JavaConverters._


/**
  * Created by allwefantasy on 5/2/2018.
  * This Module support training or predicting with user-defined python script
  */
class SQLPythonAlg(override val uid: String) extends SQLAlg with Functions with SQLPythonAlgParams with BaseAlg {

  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    pythonCheckRequirements(df)
    autoConfigureAutoCreateProjectParams(params)
    var newParams = params
    if (get(scripts).isDefined) {
      val autoCreateMLproject = new AutoCreateMLproject($(scripts), $(condaFile), $(entryPoint), $(batchPredictEntryPoint), $(apiPredictEntryPoint))
      val projectPath = autoCreateMLproject.saveProject(df.sparkSession, path)

      newParams = params
      newParams += ("enableDataLocal" -> "true")
      newParams += ("pythonScriptPath" -> projectPath)
      newParams += ("pythonDescPath" -> projectPath)
    }
    if (!params.contains("generateProjectOnly") || !params("generateProjectOnly").toBoolean) {
      new PythonTrain().train(df, path, newParams)
    } else {
      emptyDataFrame(df.sparkSession, "value")
    }

  }

  override def load(sparkSession: SparkSession, _path: String, params: Map[String, String]): Any = {

    if (!isModelPath(_path)) throw new MLSQLException(s"${_path} is not a validate model path")

    new PythonLoad().load(sparkSession, _path, params)
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    new APIPredict().predict(sparkSession, _model.asInstanceOf[ModelMeta], name, params)
  }


  override def batchPredict(df: DataFrame, _path: String, params: Map[String, String]): DataFrame = {
    if (!isModelPath(_path)) throw new MLSQLException(s"${_path} is not a validate model path")
    val bp = new BatchPredict()
    bp.predict(df, _path, params)
  }


  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession, () => {
      new SQLPythonAlg()
    })
  }

  override def explainModel(sparkSession: SparkSession, path: String, params: Map[String, String]): DataFrame = super.explainModel(sparkSession, path, params)

  override def skipPathPrefix: Boolean = false

  override def modelType: ModelType = AlgType

  override def doc: Doc = PythonAlgDoc.doc

  override def codeExample: Code = PythonAlgCodeExample.codeExample

  override def coreCompatibility: Seq[CoreVersion] = {
    Seq(Core_2_2_x, Core_2_3_x, Core_2_4_x)
  }


}

object SQLPythonAlg extends Logging with WowLog {
  def createNewFeatures(list: List[Row], inputCol: String): Matrix = {
    val numRows = list.size
    val numCols = list.head.getAs[Vector](inputCol).size
    val values = new ArrayList[Double](numCols * numRows)

    val vectorArray = list.map(r => {
      r.getAs[Vector](inputCol).toArray
    })
    for (i <- (0 until numCols)) {
      for (j <- (0 until numRows)) {
        values.add(vectorArray(j)(i))
      }
    }
    Matrices.dense(numRows, numCols, values.asScala.toArray).toSparse
  }

  def arrayParamsWithIndex(name: String, params: Map[String, String]): Array[(Int, Map[String, String])] = {
    params.filter(f => f._1.startsWith(name + ".")).map { f =>
      val Array(name, group, keys@_*) = f._1.split("\\.")
      (group, keys.mkString("."), f._2)
    }.groupBy(f => f._1).map(f => {
      val params = f._2.map(k => (k._2, k._3)).toMap
      (f._1.toInt, params)
    }).toArray
  }

  def distributeResource(spark: SparkSession, path: String, tempLocalPath: String) = {
    val psDriverBackend = PlatformManager.getRuntime.asInstanceOf[SparkRuntime].psDriverBackend
    psDriverBackend.psDriverRpcEndpointRef.askSync[Boolean](Message.CopyModelToLocal(path, tempLocalPath))
  }

  def isAPIService() = {
    val runtimeParams = PlatformManager.getRuntime.params.asScala.toMap
    runtimeParams.getOrElse("streaming.deploy.rest.api", "false").toString.toBoolean
  }

  def distributePythonProject(spark: SparkSession, localProjectDirectory: String, pythonProjectPath: Option[String]): Option[String] = {


    if (pythonProjectPath.isDefined) {
      logInfo(format(s"system load python project into directory: [ ${
        localProjectDirectory
      } ]."))
      distributeResource(spark, pythonProjectPath.get, localProjectDirectory)
      logInfo(format("python project loaded!"))
      Some(localProjectDirectory)
    } else {
      None
    }
  }

  def downloadPythonProject(localProjectDirectory: String, pythonProjectPath: Option[String]) = {

    if (pythonProjectPath.isDefined) {
      logInfo(format(s"system load python project into directory: [ ${
        localProjectDirectory
      } ]."))
      HDFSOperator.copyToLocalFile(localProjectDirectory, pythonProjectPath.get, true)
      logInfo(format("python project loaded!"))
      Some(localProjectDirectory)
    }
    else None

  }

}
