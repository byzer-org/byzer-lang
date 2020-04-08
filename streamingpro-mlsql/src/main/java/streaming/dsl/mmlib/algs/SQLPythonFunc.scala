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

import java.nio.file.Paths
import java.util.UUID

import org.apache.spark.sql.SparkSession
import streaming.dsl.mmlib.algs.python.{MLFlow, PythonScript}
import tech.mlsql.common.utils.hdfs.HDFSOperator

/**
 * Created by allwefantasy on 1/2/2018.
 */
object SQLPythonFunc {

  def getPath(params: Map[String, String]) = {
    if (params.contains("pythonDescPath") || params.contains("pythonScriptPath")) {
      Some(params.getOrElse("pythonDescPath", params.getOrElse("pythonScriptPath", "")))
    } else None
  }

  def loadUserDefinePythonScript(params: Map[String, String], spark: SparkSession) = {
    getPath(params) match {
      case Some(path) =>
        if (HDFSOperator.isDir(path) && HDFSOperator.fileExists(Paths.get(path, "MLproject").toString)) {
          val project = path.split("/").last
          Some(PythonScript("", project, path, "", MLFlow))

        } else {
          val pathChunk = path.split("/")
          val userFileName = pathChunk.last
          val userPythonScriptList = spark.sparkContext.textFile(path, 1).collect().mkString("\n")
          Some(PythonScript(userFileName, userPythonScriptList, "", path))
        }

      case None => None
    }
  }


  def recordUserLog(algIndex: Int, pythonScript: PythonScript, kafkaParam: Map[String, String], res: Iterator[String],
                    logCallback: (String) => Unit = (msg: String) => {}) = {
    val logPrefix = algIndex + "/" + pythonScript.filePath + ":  "
    val scores = writeLog(logPrefix, kafkaParam, res, logCallback)
    val score = if (scores.size > 0) scores.head else 0d
    score
  }

  def recordAnyLog(kafkaParam: Map[String, String], logCallback: (String) => Unit = (msg: String) => {}) = {
    val a = (line: Any) => {
      line match {
        case a: Iterator[String] => recordMultiLineLog(kafkaParam, line.asInstanceOf[Iterator[String]], logCallback)
        case a: Exception => recordUserException(kafkaParam, line.asInstanceOf[Exception], logCallback)
        case _ => recordSingleLineLog(kafkaParam, line.asInstanceOf[String], logCallback)
      }
    }
    a
  }

  def recordSingleLineLog(kafkaParam: Map[String, String], line: String, logCallback: (String) => Unit = (msg: String) => {}) = {
    writeLog("", kafkaParam, Seq(line).toIterator, logCallback)
  }

  def recordMultiLineLog(kafkaParam: Map[String, String], res: Iterator[String], logCallback: (String) => Unit = (msg: String) => {}) = {
    writeLog("", kafkaParam, res, logCallback)
  }

  def recordUserException(kafkaParam: Map[String, String], e: Exception, logCallback: (String) => Unit = (msg: String) => {}) = {
    writeLog("", kafkaParam, Seq(e.getStackTrace.map { f =>
      logCallback(f.toString)
      f.toString
    }.mkString("\n")).toIterator)
  }

  def writeLog(prefix: String, kafkaParam: Map[String, String], lines: Iterator[String], logCallback: (String) => Unit = (msg: String) => {}) = {
    def filterScore(str: String) = {
      if (str != null && str.startsWith("mlsql_validation_score:")) {
        str.split(":").last.toDouble
      } else 0d
    }
    lines.map {
      f =>
        logCallback(prefix + "" + f)
        filterScore(f)
    }.filter(f => f > 0d).toSeq
  }


  def findPythonPredictScript(sparkSession: SparkSession,
                              params: Map[String, String],
                              defaultScriptName: String
                             ) = {
    val userPythonScript = loadUserDefinePythonScript(params, sparkSession)
    userPythonScript.get

  }

  // --  path related (local/hdfs)

  def getLocalTempModelPath(hdfsPath: String) = {
    s"${getLocalBasePath}/${UUID.randomUUID().toString}/models"
  }

  def localOutputPath(hdfsPath: String) = {
    s"${getLocalBasePath}/${UUID.randomUUID().toString}/output"
  }

  def getLocalTempDataPath(hdfsPath: String) = {
    s"${getLocalBasePath}/${UUID.randomUUID().toString}/data"
  }

  def getLocalRunPath(hdfsPath: String) = {
    s"${getLocalBasePath}/${UUID.randomUUID().toString}/mlsqlrun"
  }

  def getLocalTempResourcePath(hdfsPath: String, resourceName: String) = {
    s"${getLocalBasePath}/${UUID.randomUUID()}/resource/${resourceName}"
  }

  def getLocalBasePath = {
    s"/tmp/__mlsql__"
  }


  def getAlgModelPath(hdfsPath: String, versionEnabled: Boolean = false) = {
    s"${getAlgBasePath(hdfsPath, versionEnabled)}/model"
  }


  def getAlgModelPathWithVersion(hdfsPath: String, version: Int) = {
    s"${getAlgBasePathWithVersion(hdfsPath, version)}/model"
  }

  def incrementVersion(basePath: String, versionEnabled: Boolean) = {
    if (versionEnabled) {
      val maxVersion = getModelVersion(basePath)
      val path = maxVersion match {
        case Some(v) => s"${basePath}/_model_${v + 1}"
        case None => s"${basePath}/_model_0"
      }
      HDFSOperator.createDir(path)
    }
  }

  def getAlgBasePath(hdfsPath: String, versionEnabled: Boolean = false) = {

    val basePath = hdfsPath
    if (versionEnabled) {
      val maxVersion = getModelVersion(basePath)
      maxVersion match {
        case Some(v) => s"${basePath}/_model_${v}"
        case None => s"${basePath}/_model_0"
      }
    }
    else {
      basePath
    }
  }

  def getAlgBasePathWithVersion(hdfsPath: String, version: Int) = {
    s"${hdfsPath}/_model_${version}"
  }

  def getModelVersion(basePath: String) = {
    try {
      HDFSOperator.listModelDirectory(basePath).filter(f => f.getPath.getName.startsWith("_model_")).
        map(f => f.getPath.getName.split("_").last.toInt).sorted.reverse.headOption
    } catch {
      case e: Exception =>
        None
    }
  }

  def getAlgMetalPath(hdfsPath: String, versionEnabled: Boolean = false) = {
    s"${getAlgBasePath(hdfsPath, versionEnabled)}/meta"
  }

  def getAlgMetalPathWithVersion(hdfsPath: String, version: Int) = {
    s"${getAlgBasePathWithVersion(hdfsPath, version)}/meta"
  }

  // tmp no need to keep version
  def getAlgTmpPath(hdfsPath: String) = {
    s"${hdfsPath}/tmp"
  }

  // -- path related
}
