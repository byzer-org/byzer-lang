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

package streaming.dsl.mmlib.algs.python

import java.io.{File, FileWriter}
import java.util
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.util._
import streaming.common.ScalaMethodMacros._
import streaming.common.{HDFSOperator, NetUtils}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.SQLPythonFunc._
import streaming.dsl.mmlib.algs.{Functions, SQLPythonAlg, SQLPythonFunc}

import scala.collection.JavaConverters._

class PythonTrain extends Functions with Serializable {
  def train_per_partition(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val keepVersion = params.getOrElse("keepVersion", "false").toBoolean
    val keepLocalDirectory = params.getOrElse("keepLocalDirectory", "false").toBoolean

    val partitionKey = params.get("partitionKey")

    var kafkaParam = mapParams("kafkaParam", params)

    val enableDataLocal = params.getOrElse("enableDataLocal", "true").toBoolean

    var stopFlagNum = -1
    if (!enableDataLocal) {
      require(kafkaParam.size > 0, "We detect that you do not set enableDataLocal true, " +
        "in this situation, kafkaParam should be configured")
      val (_kafkaParam, _newRDD) = writeKafka(df, path, params)
      stopFlagNum = _newRDD.getNumPartitions
      kafkaParam = _kafkaParam
    }

    // find python project
    val pythonProject = PythonAlgProject.loadProject(params, df.sparkSession)
    incrementVersion(path, keepVersion)

    val mlsqlContext = ScriptSQLExec.contextGetOrForTest()

    val pythonProjectPath = Option(pythonProject.get.filePath)

    val projectName = pythonProject.get.projectName
    val projectType = pythonProject.get.scriptType

    var fitParam = mapParams("fitParam", params)


    def cleanNumber() = {
      fitParam.map { f =>
        var key = f._1
        val value = f._2
        try {
          val prefix = key.split("\\.")(0)
          (prefix).toLong
          key = key.substring(prefix.length + 1)
        } catch {
          case e: Exception =>
        }
        (key, value)

      }
    }

    fitParam = cleanNumber()

    val systemParam = mapParams("systemParam", params)
    val mlflowConfig = MLFlowConfig.buildFromSystemParam(systemParam)
    val pythonConfig = PythonConfig.buildFromSystemParam(systemParam)

    val appName = df.sparkSession.sparkContext.getConf.get("spark.app.name")

    val envs = EnvConfig.buildFromSystemParam(systemParam) ++ Map(BasicCondaEnvManager.MLSQL_INSTNANCE_NAME_KEY -> appName)


    if (!keepVersion) {
      if (path.contains("..") || path == "/" || path.split("/").length < 3) {
        throw new MLSQLException("path should at least three layer")
      }
      HDFSOperator.deleteDir(SQLPythonFunc.getAlgModelPath(path, keepVersion))
    }

    val wowRDD = df.toJSON.rdd.mapPartitionsWithIndex { case (algIndex, iter) =>

      ScriptSQLExec.setContext(mlsqlContext)

      val localPathConfig = LocalPathConfig.buildFromParams(path)
      var tempDataLocalPathWithAlgSuffix = localPathConfig.localDataPath

      if (enableDataLocal) {
        tempDataLocalPathWithAlgSuffix = tempDataLocalPathWithAlgSuffix + "/" + algIndex
        val msg = s"dataLocalFormat enabled ,system will generate data in ${tempDataLocalPathWithAlgSuffix} "
        logInfo(format(msg))
        if (!new File(tempDataLocalPathWithAlgSuffix).exists()) {
          FileUtils.forceMkdir(new File(tempDataLocalPathWithAlgSuffix))
        }
        val localFile = new File(tempDataLocalPathWithAlgSuffix, UUID.randomUUID().toString + ".json")
        val localFileWriter = new FileWriter(localFile)
        try {
          iter.foreach { line =>
            localFileWriter.write(line + "\n")
          }
        } finally {
          localFileWriter.close()
        }
      }

      val paramMap = new util.HashMap[String, Object]()
      var item = fitParam.asJava
      if (!fitParam.contains("modelPath")) {
        item = (fitParam + ("modelPath" -> path)).asJava
      }

      val resourceParams = new ResourceManager(fitParam).loadResourceInTrain

      val taskDirectory = localPathConfig.localRunPath + "/" + projectName
      val tempModelLocalPath = s"${localPathConfig.localModelPath}/${algIndex}"

      paramMap.put("fitParam", item)

      if (!kafkaParam.isEmpty) {
        val kafkaP = kafkaParam + ("group_id" -> (kafkaParam("group_id") + "_" + algIndex))
        paramMap.put("kafkaParam", kafkaP.asJava)
      }

      val internalSystemParam = Map(
        str[RunPythonConfig.InternalSystemParam](_.stopFlagNum) -> stopFlagNum,
        str[RunPythonConfig.InternalSystemParam](_.tempModelLocalPath) -> tempModelLocalPath,
        str[RunPythonConfig.InternalSystemParam](_.tempDataLocalPath) -> tempDataLocalPathWithAlgSuffix,
        str[RunPythonConfig.InternalSystemParam](_.resource) -> resourceParams.asJava
      )

      paramMap.put(RunPythonConfig.internalSystemParam, internalSystemParam.asJava)

      pythonProjectPath match {
        case Some(_) =>
          if (projectType == MLFlow) {
            logInfo(format(s"'${pythonProjectPath.get}' is MLflow project. download it from [${pythonProject}] to local [${taskDirectory}]"))
            SQLPythonAlg.downloadPythonProject(taskDirectory, pythonProjectPath)
          } else {
            val localPath = taskDirectory + "/" + pythonProjectPath.get.split("/").last
            logInfo(format(s"'${pythonProjectPath.get}' is Normal project. download it from [${pythonProject}] to local [${localPath}]"))
            SQLPythonAlg.downloadPythonProject(localPath, pythonProjectPath)
          }


        case None =>
          // this will not happen, cause even script is a project contains only one python script file.
          throw new RuntimeException("The project or script you configured in pythonScriptPath is not a validate project")
      }


      val command = new PythonAlgExecCommand(pythonProject.get, None, None, envs).
        generateCommand(MLProject.train_command)


      val execDesc =
        s"""
           |
           |----------------------------------------------------------------
           |host: ${NetUtils.getHost}
           |
           |Command: ${command.mkString(" ")}
           |TaskDirectory: ${taskDirectory}
           |DataDirectory: ${tempDataLocalPathWithAlgSuffix}
           |ModelDirectory: ${tempModelLocalPath}
           |
           |Notice:
           |
           |If you wanna keep [${taskDirectory}] for debug, please set
           |keepLocalDirectory=true in train statement.
           |
           |e.g:
           |
           |train data as PythonParallelExt.`/tmp/abc` where keepLocalDirectory="true";
           |----------------------------------------------------------------
         """.stripMargin
      logInfo(format(execDesc))

      val modelTrainStartTime = System.currentTimeMillis()

      var score = 0.0
      var message = ""
      var trainFailFlag = false
      val runner = new PythonProjectExecuteRunner(
        taskDirectory = taskDirectory,
        keepLocalDirectory = keepLocalDirectory,
        envVars = envs,
        logCallback = (msg) => {
          ScriptSQLExec.setContextIfNotPresent(mlsqlContext)
          val info = format(msg)
          logInfo(info)
          message += (info + "\n")
        }
      )
      try {

        val res = runner.run(
          command = command,
          params = paramMap,
          schema = MapType(StringType, MapType(StringType, StringType)),
          scriptContent = pythonProject.get.fileContent,
          scriptName = pythonProject.get.fileName,
          validateData = Array()
        )

        def filterScore(str: String) = {
          if (str != null && str.startsWith("mlsql_validation_score:")) {
            str.split(":").last.toDouble
          } else 0d
        }

        val scores = res.map { f =>
          logInfo(format(f))
          message += f
          filterScore(f)
        }.toSeq
        score = if (scores.size > 0) scores.head else 0d
      } catch {
        case e: Exception =>
          message += format_cause(e)
          logError(message)
          trainFailFlag = true
      }

      val modelTrainEndTime = System.currentTimeMillis()

      val partitionName = partitionKey match {
        case Some(i) => s"${i}=" + algIndex
        case None => algIndex
      }

      val modelHDFSPath = SQLPythonFunc.getAlgModelPath(path, keepVersion) + "/" + partitionName
      try {
        // If training failed, we do not need
        // copy model to hdfs
        if (!trainFailFlag) {
          //copy model to HDFS
          val fs = FileSystem.get(new Configuration())
          if (!keepVersion) {
            fs.delete(new Path(modelHDFSPath), true)
          }
          fs.copyFromLocalFile(new Path(tempModelLocalPath),
            new Path(modelHDFSPath))
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
          trainFailFlag = true
      } finally {
        // delete resource
        resourceParams.foreach { rp =>
          FileUtils.deleteQuietly(new File(rp._2))
        }
        // delete local model
        FileUtils.deleteQuietly(new File(tempModelLocalPath))
        // delete local data
        if (!keepLocalDirectory) {
          FileUtils.deleteQuietly(new File(tempDataLocalPathWithAlgSuffix))
        }
      }
      val metrics = Seq(Row.fromSeq(Seq("validateScore", score)))
      val status = if (trainFailFlag) "fail" else "success"
      val row = Row.fromSeq(Seq(
        modelHDFSPath,
        algIndex,
        pythonProject.get.fileName,
        score,
        metrics,
        status,
        message,
        modelTrainStartTime,
        modelTrainEndTime,
        fitParam,
        execDesc
      ))
      Seq(row).toIterator
    }

    df.sparkSession.createDataFrame(wowRDD, PythonTrainingResultSchema.algSchema).write.mode(SaveMode.Overwrite).parquet(SQLPythonFunc.getAlgMetalPath(path, keepVersion) + "/0")

    val tempRDD = df.sparkSession.sparkContext.parallelize(Seq(Seq(
      Map(
        str[PythonConfig](_.pythonPath) -> pythonConfig.pythonPath,
        str[PythonConfig](_.pythonVer) -> pythonConfig.pythonVer
      ), params)), 1).map { f =>
      Row.fromSeq(f)
    }
    df.sparkSession.createDataFrame(tempRDD, PythonTrainingResultSchema.trainParamsSchema).
      write.
      mode(SaveMode.Overwrite).
      parquet(SQLPythonFunc.getAlgMetalPath(path, keepVersion) + "/1")

    df.sparkSession.read.parquet(SQLPythonFunc.getAlgMetalPath(path, keepVersion) + "/0")
  }

  def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    val keepVersion = params.getOrElse("keepVersion", "true").toBoolean
    val keepLocalDirectory = params.getOrElse("keepLocalDirectory", "false").toBoolean

    var kafkaParam = mapParams("kafkaParam", params)

    // save data to hdfs and broadcast validate table
    val dataManager = new DataManager(df, path, params)
    val enableDataLocal = dataManager.enableDataLocal
    val dataHDFSPath = dataManager.saveDataToHDFS
    val rowsBr = dataManager.broadCastValidateTable

    var stopFlagNum = -1
    if (!enableDataLocal) {
      require(kafkaParam.size > 0, "We detect that you do not set enableDataLocal true, " +
        "in this situation, kafkaParam should be configured")
      val (_kafkaParam, _newRDD) = writeKafka(df, path, params)
      stopFlagNum = _newRDD.getNumPartitions
      kafkaParam = _kafkaParam
    }

    val systemParam = mapParams("systemParam", params)
    var fitParam = arrayParamsWithIndex("fitParam", params)

    if (fitParam.size == 0) {
      logWarning(format("fitParam is not configured, we will use empty configuration"))
      fitParam = Array(0 -> Map[String, String]())
    }
    val fitParamRDD = df.sparkSession.sparkContext.parallelize(fitParam, fitParam.length)


    //configuration about project and env
    val mlflowConfig = MLFlowConfig.buildFromSystemParam(systemParam)
    val pythonConfig = PythonConfig.buildFromSystemParam(systemParam)

    val appName = df.sparkSession.sparkContext.getConf.get("spark.app.name")
    val envs = EnvConfig.buildFromSystemParam(systemParam) ++ Map(BasicCondaEnvManager.MLSQL_INSTNANCE_NAME_KEY -> appName)


    // find python project
    val pythonProject = PythonAlgProject.loadProject(params, df.sparkSession)


    incrementVersion(path, keepVersion)

    val mlsqlContext = ScriptSQLExec.contextGetOrForTest()

    val pythonProjectPath = Option(pythonProject.get.filePath)

    val projectName = pythonProject.get.projectName
    val projectType = pythonProject.get.scriptType

    val wowRDD = fitParamRDD.map { paramAndIndex =>

      ScriptSQLExec.setContext(mlsqlContext)


      val f = paramAndIndex._2
      val algIndex = paramAndIndex._1

      val localPathConfig = LocalPathConfig.buildFromParams(path)
      var tempDataLocalPathWithAlgSuffix = localPathConfig.localDataPath

      if (enableDataLocal) {
        tempDataLocalPathWithAlgSuffix = tempDataLocalPathWithAlgSuffix + "/" + algIndex
        val msg = s"dataLocalFormat enabled ,system will generate data in ${tempDataLocalPathWithAlgSuffix} "
        logInfo(format(msg))
        HDFSOperator.copyToLocalFile(tempLocalPath = tempDataLocalPathWithAlgSuffix, path = dataHDFSPath, true)
      }

      val paramMap = new util.HashMap[String, Object]()
      var item = f.asJava
      if (!f.contains("modelPath")) {
        item = (f + ("modelPath" -> path)).asJava
      }

      val resourceParams = new ResourceManager(f).loadResourceInTrain

      val taskDirectory = localPathConfig.localRunPath + "/" + projectName
      val tempModelLocalPath = s"${localPathConfig.localModelPath}/${algIndex}"

      paramMap.put("fitParam", item)

      if (!kafkaParam.isEmpty) {
        val kafkaP = kafkaParam + ("group_id" -> (kafkaParam("group_id") + "_" + algIndex))
        paramMap.put("kafkaParam", kafkaP.asJava)
      }

      val internalSystemParam = Map(
        str[RunPythonConfig.InternalSystemParam](_.stopFlagNum) -> stopFlagNum,
        str[RunPythonConfig.InternalSystemParam](_.tempModelLocalPath) -> tempModelLocalPath,
        str[RunPythonConfig.InternalSystemParam](_.tempDataLocalPath) -> tempDataLocalPathWithAlgSuffix,
        str[RunPythonConfig.InternalSystemParam](_.resource) -> resourceParams.asJava
      )

      paramMap.put(RunPythonConfig.internalSystemParam, internalSystemParam.asJava)
      paramMap.put(RunPythonConfig.systemParam, systemParam.asJava)

      pythonProjectPath match {
        case Some(_) =>
          if (projectType == MLFlow) {
            logInfo(format(s"'${pythonProjectPath.get}' is MLflow project. download it from [${pythonProject}] to local [${taskDirectory}]"))
            SQLPythonAlg.downloadPythonProject(taskDirectory, pythonProjectPath)
          } else {
            val localPath = taskDirectory + "/" + pythonProjectPath.get.split("/").last
            logInfo(format(s"'${pythonProjectPath.get}' is Normal project. download it from [${pythonProject}] to local [${localPath}]"))
            SQLPythonAlg.downloadPythonProject(localPath, pythonProjectPath)
          }


        case None =>
          // this will not happen, cause even script is a project contains only one python script file.
          throw new RuntimeException("The project or script you configured in pythonScriptPath is not a validate project")
      }


      val command = new PythonAlgExecCommand(pythonProject.get, Option(mlflowConfig), Option(pythonConfig), envs).
        generateCommand(MLProject.train_command)


      val execDesc =
        s"""
           |
           |----------------------------------------------------------------
           |host: ${NetUtils.getHost}
           |
           |Command: ${command.mkString(" ")}
           |TaskDirectory: ${taskDirectory}
           |DataDirectory: ${tempDataLocalPathWithAlgSuffix}
           |ModelDirectory: ${tempModelLocalPath}
           |
           |Notice:
           |
           |If you wanna keep [${taskDirectory}] for debug, please set
           |keepLocalDirectory=true in train statement.
           |
           |e.g:
           |
           |train data as PythonAlg.`/tmp/abc` where keepLocalDirectory="true";
           |----------------------------------------------------------------
         """.stripMargin
      logInfo(format(execDesc))

      val modelTrainStartTime = System.currentTimeMillis()

      var score = 0.0
      var message = ""
      var trainFailFlag = false
      val runner = new PythonProjectExecuteRunner(
        taskDirectory = taskDirectory,
        keepLocalDirectory = keepLocalDirectory,
        envVars = envs,
        logCallback = (msg) => {
          ScriptSQLExec.setContextIfNotPresent(mlsqlContext)
          val info = format(msg)
          logInfo(info)
          message += (info + "\n")
        }
      )
      try {

        val res = runner.run(
          command = command,
          params = paramMap,
          schema = MapType(StringType, MapType(StringType, StringType)),
          scriptContent = pythonProject.get.fileContent,
          scriptName = pythonProject.get.fileName,
          validateData = rowsBr.value
        )

        def filterScore(str: String) = {
          if (str != null && str.startsWith("mlsql_validation_score:")) {
            str.split(":").last.toDouble
          } else 0d
        }

        val scores = res.map { f =>
          logInfo(format(f))
          message += f
          filterScore(f)
        }.toSeq
        score = if (scores.size > 0) scores.filter(f => f > 0.0).head else 0d
      } catch {
        case e: Exception =>
          message += format_cause(e)
          logError(message)
          trainFailFlag = true
      }

      val modelTrainEndTime = System.currentTimeMillis()

      val modelHDFSPath = SQLPythonFunc.getAlgModelPath(path, keepVersion) + "/" + algIndex
      try {
        // If training failed, we do not need
        // copy model to hdfs
        if (!trainFailFlag) {
          //copy model to HDFS
          val fs = FileSystem.get(new Configuration())
          if (!keepVersion) {
            fs.delete(new Path(modelHDFSPath), true)
          }
          fs.copyFromLocalFile(new Path(tempModelLocalPath),
            new Path(modelHDFSPath))
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
          trainFailFlag = true
      } finally {
        // delete local model
        FileUtils.deleteQuietly(new File(tempModelLocalPath))
        // delete local data
        if (!keepLocalDirectory) {
          FileUtils.deleteQuietly(new File(tempDataLocalPathWithAlgSuffix))
        }
        // delete resource
        resourceParams.foreach { rp =>
          FileUtils.deleteQuietly(new File(rp._2))
        }
      }
      val status = if (trainFailFlag) "fail" else "success"

      val metrics = Seq(Row.fromSeq(Seq("validateScore", score)))

      Row.fromSeq(Seq(
        modelHDFSPath,
        algIndex,
        pythonProject.get.fileName,
        score,
        metrics,
        status,
        message,
        modelTrainStartTime,
        modelTrainEndTime,
        f,
        execDesc
      ))
    }

    df.sparkSession.createDataFrame(wowRDD, PythonTrainingResultSchema.algSchema).write.
      mode(SaveMode.Overwrite).
      parquet(SQLPythonFunc.getAlgMetalPath(path, keepVersion) + "/0")

    val tempRDD = df.sparkSession.sparkContext.parallelize(Seq(Seq(
      Map(
        str[PythonConfig](_.pythonPath) -> pythonConfig.pythonPath,
        str[PythonConfig](_.pythonVer) -> pythonConfig.pythonVer
      ), params)), 1).map { f =>
      Row.fromSeq(f)
    }
    df.sparkSession.createDataFrame(tempRDD, PythonTrainingResultSchema.trainParamsSchema).
      write.
      mode(SaveMode.Overwrite).
      parquet(SQLPythonFunc.getAlgMetalPath(path, keepVersion) + "/1")

    df.sparkSession.read.parquet(SQLPythonFunc.getAlgMetalPath(path, keepVersion) + "/0")
  }


}


