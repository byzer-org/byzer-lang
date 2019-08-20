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

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.json.WowJsonInferSchema
import org.apache.spark.sql.types.{MapType, StringType}
import org.apache.spark.util.PythonProjectExecuteRunner
import tech.mlsql.common.utils.lang.sc.ScalaMethodMacros.str
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.{Functions, SQLPythonAlg, SQLPythonFunc}
import streaming.log.WowLog
import tech.mlsql.common.utils.env.python.BasicCondaEnvManager
import tech.mlsql.common.utils.hdfs.HDFSOperator
import tech.mlsql.common.utils.log.Logging

import scala.collection.JavaConverters._

class BatchPredict extends Logging with WowLog with Serializable {
  def predict(df: DataFrame, _path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession

    val keepLocalDirectory = params.getOrElse("keepLocalDirectory", "false").toBoolean
    val modelMetaManager = new ModelMetaManager(spark, _path, params)
    val modelMeta = modelMetaManager.loadMetaAndModel(null, Map())
    var (selectedFitParam, resourceParams, modelHDFSToLocalPath) = new ResourceManager(params).loadResourceInRegister(spark, modelMeta)

    modelMeta.copy(resources = selectedFitParam, modelHDFSToLocalPath = modelHDFSToLocalPath)
    val resources = modelMeta.resources

    // if pythonScriptPath is defined in predict/run, then use it otherwise find them in train params.
    val pythonProject = PythonAlgProject.getPythonScriptPath(params) match {
      case Some(p) => PythonAlgProject.loadProject(params, df.sparkSession)
      case None => PythonAlgProject.loadProject(modelMeta.trainParams, df.sparkSession)
    }
    val projectName = pythonProject.get.projectName

    val systemParam = Functions.mapParams("systemParam", modelMeta.trainParams)

    val mlsqlContext = ScriptSQLExec.contextGetOrForTest()

    val schema = df.schema
    val sessionLocalTimeZone = df.sparkSession.sessionState.conf.sessionLocalTimeZone

    val modelPath = modelMeta.modelEntityPaths.head

    val outoutFile = SQLPythonFunc.getAlgTmpPath(_path) + "/output"
    HDFSOperator.deleteDir(outoutFile)
    HDFSOperator.createDir(outoutFile)


    val trainParams = modelMeta.trainParams
    val appName = df.sparkSession.sparkContext.getConf.get("spark.app.name")

    val jsonRDD = df.rdd.mapPartitionsWithIndex { (index, iter) =>
      ScriptSQLExec.setContext(mlsqlContext)
      val mlflowConfig = MLFlowConfig.buildFromSystemParam(systemParam)
      val pythonConfig = PythonConfig.buildFromSystemParam(systemParam)

      val envs = EnvConfig.buildFromSystemParam(systemParam) ++ Map(BasicCondaEnvManager.MLSQL_INSTNANCE_NAME_KEY -> appName)


      val command = new PythonAlgExecCommand(pythonProject.get, Option(mlflowConfig), Option(pythonConfig), envs).
        generateCommand(MLProject.batch_predict_command)

      val localPathConfig = LocalPathConfig.buildFromParams(_path)

      val localDataFile = new File(localPathConfig.localDataPath)
      if (!localDataFile.exists()) {
        localDataFile.mkdirs()
      }

      val fileWriter = new FileWriter(new File(localPathConfig.localDataPath + s"/${index}.json"))
      try {
        WowJsonInferSchema.toJson(iter, schema, sessionLocalTimeZone, callback = (row) => {
          fileWriter.write(row + "\n")
        })
      } catch {
        case e: Exception =>
          logError(format_exception(e))
      } finally {
        fileWriter.close()
      }

      val paramMap = new util.HashMap[String, Object]()

      val localModelPath = localPathConfig.localModelPath + s"/${index}"
      HDFSOperator.copyToLocalFile(localModelPath, modelPath, true)

      val localOutputFileStr = localPathConfig.localOutputPath + s"/output-${index}.json"

      val internalSystemParam = Map(
        str[RunPythonConfig.InternalSystemParam](_.tempDataLocalPath) -> (localPathConfig.localDataPath + s"/${index}.json"),
        str[RunPythonConfig.InternalSystemParam](_.tempOutputLocalPath) -> localOutputFileStr,
        str[RunPythonConfig.InternalSystemParam](_.tempModelLocalPath) -> localModelPath
      ) ++ modelMeta.resources


      val localOutputPathFile = new File(localPathConfig.localOutputPath)
      if (!localOutputPathFile.exists()) {
        localOutputPathFile.mkdirs()
      }

      paramMap.put(RunPythonConfig.internalSystemParam, (resources ++ internalSystemParam).asJava)
      paramMap.put(RunPythonConfig.systemParam, systemParam.asJava)
      paramMap.put("trainParams", trainParams.asJava)
      paramMap.put("fitParams", params.asJava)

      val taskDirectory = localPathConfig.localRunPath + "/" + projectName

      SQLPythonAlg.downloadPythonProject(taskDirectory, Option(pythonProject.get.filePath))
      var message = ""
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
      var trainFailFlag = false
      try {

        val res = runner.run(
          command = command,
          params = paramMap,
          schema = MapType(StringType, MapType(StringType, StringType)),
          scriptContent = pythonProject.get.fileContent,
          scriptName = pythonProject.get.fileName,
          validateData = Array()
        )
        res.foreach(f => logInfo(format(f)))

        HDFSOperator.copyToHDFS(localOutputFileStr, outoutFile, cleanTarget = false, cleanSource = false)

      } catch {
        case e: Exception =>
          val info = format_cause(e)
          logError(info)
          trainFailFlag = true
      } finally {
        FileUtils.deleteDirectory(new File(localModelPath))
        FileUtils.deleteDirectory(new File(localPathConfig.localDataPath))
        FileUtils.deleteDirectory(new File(localPathConfig.localOutputPath))
      }
      List[String]().toIterator
    }.count()
    spark.read.json(outoutFile)
  }

}
