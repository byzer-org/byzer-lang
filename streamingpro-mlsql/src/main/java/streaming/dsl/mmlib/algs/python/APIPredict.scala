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

import java.io.File
import java.nio.file.{Files, Paths}
import java.util
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.apache.spark.api.python.WowPythonRunner
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType}
import org.apache.spark.sql.{MLSQLUtils, SparkSession}
import org.apache.spark.util.ObjPickle.{pickleInternalRow, unpickle}
import org.apache.spark.util.VectorSerDer.{ser_vector, vector_schema}
import org.apache.spark.util.{PredictTaskContext, PythonProjectExecuteRunner, VectorSerDer}
import org.apache.spark.{APIDeployPythonRunnerEnv, SparkCoreVersion}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.{Functions, SQLPythonAlg}
import streaming.log.WowLog
import tech.mlsql.common.utils.env.python.BasicCondaEnvManager
import tech.mlsql.common.utils.log.Logging

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class APIPredict extends Logging with WowLog with Serializable {
  def predict(sparkSession: SparkSession, modelMeta: ModelMeta, name: String, params: Map[String, String]): UserDefinedFunction = {
    val models = sparkSession.sparkContext.broadcast(modelMeta.modelEntityPaths)
    val trainParams = modelMeta.trainParams
    val systemParam = Functions.mapParams("systemParam", trainParams)


    val pythonConfig = PythonConfig.buildFromSystemParam(systemParam)


    // if pythonScriptPath is defined in predict/run, then use it otherwise find them in train params.
    val pythonProject = PythonAlgProject.getPythonScriptPath(params) match {
      case Some(p) => PythonAlgProject.loadProject(params, sparkSession)
      case None => PythonAlgProject.loadProject(modelMeta.trainParams, sparkSession)
    }

    val maps = new util.HashMap[String, java.util.Map[String, _]]()
    val item = new util.HashMap[String, String]()
    val funcSerLocation = "/tmp/__mlsql__/" + UUID.randomUUID().toString
    item.put("funcPath", funcSerLocation)
    maps.put("systemParam", item)
    maps.put("internalSystemParam", modelMeta.resources.asJava)

    val mlsqlContext = ScriptSQLExec.contextGetOrForTest()

    val recordLog = (msg: String) => {
      ScriptSQLExec.setContextIfNotPresent(mlsqlContext)
      logInfo(format(msg))
    }

    val taskDirectory = modelMeta.taskDirectory.get
    val enableCopyTrainParamsToPython = params.getOrElse("enableCopyTrainParamsToPython", "false").toBoolean

    val envs = new util.HashMap[String, String]()

    val appName = sparkSession.sparkContext.getConf.get("spark.app.name")
    envs.put(BasicCondaEnvManager.MLSQL_INSTNANCE_NAME_KEY, appName)

    EnvConfig.buildFromSystemParam(systemParam).foreach(f => envs.put(f._1, f._2))

    val pythonRunner = new PythonProjectExecuteRunner(taskDirectory = taskDirectory,
      keepLocalDirectory = false,
      envVars = envs.asScala.toMap, logCallback = recordLog)

    val apiPredictCommand = new PythonAlgExecCommand(pythonProject.get, None, Option(pythonConfig), envs.asScala.toMap).
      generateCommand(MLProject.api_predict_command)

    /*
      Run python script in driver so we can get function then broadcast it to all
      python worker.
      Make sure you use `sys.path.insert(0,mlsql.internal_system_param["resource"]["mlFlowProjectPath"])`
      if you run it in project.
     */
    val res = pythonRunner.run(
      apiPredictCommand,
      maps,
      MapType(StringType, MapType(StringType, StringType)),
      pythonProject.get.fileContent,
      pythonProject.get.fileName
    )

    res.foreach(f => f)
    val command = Files.readAllBytes(Paths.get(item.get("funcPath")))
    try {
      FileUtils.forceDelete(new File(funcSerLocation))
    } catch {
      case e: Exception =>
        logError(s"API predict command is not stored in ${funcSerLocation}. Maybe there are something wrong when serializable predict command?", e)
    }


    def coreVersion = {
      if (SparkCoreVersion.is_2_2_X) {
        "22"
      } else if (SparkCoreVersion.is_2_3_2()) {
        "232"
      }
      else if (SparkCoreVersion.is_2_3_1()) {
        "23"
      } else if (SparkCoreVersion.is_2_4_X()) {
        "24"
      } else {
        throw new RuntimeException(s"No such spark version ${SparkCoreVersion.exactVersion}")
      }

    }

    val customDeamon = true

    def daemon = {
      if (customDeamon)
        s"-m daemon${coreVersion}"
      else "-m pyspark.daemon"
    }

    def worker: String = {
      if (customDeamon)
        s"-m worker${coreVersion}"
      else
        "-m pyspark.worker"
    }

    val (daemonCommand, workerCommand) = pythonProject.get.scriptType match {
      case MLFlow =>
        val project = MLProject.loadProject(pythonProject.get.filePath, envs.asScala.toMap)
        (Seq("bash", "-c", project.condaEnvCommand + s" && cd ${WowPythonRunner.PYSPARK_DAEMON_FILE_LOCATION} && python  ${daemon}"),
          Seq("bash", "-c", project.condaEnvCommand + s" && cd ${WowPythonRunner.PYSPARK_DAEMON_FILE_LOCATION} && python  ${worker}"))
      case _ =>
        (
          Seq("bash", "-c", s" cd ${WowPythonRunner.PYSPARK_DAEMON_FILE_LOCATION} &&" +
            s" ${pythonConfig.pythonPath}  ${daemon}"),
          Seq("bash", "-c", s" cd ${WowPythonRunner.PYSPARK_DAEMON_FILE_LOCATION} &&" +
            s" ${pythonConfig.pythonPath}  ${worker}")
        )
    }

    logInfo(format(s"daemonCommand => ${daemonCommand.mkString(" ")} workerCommand=> ${workerCommand.mkString(" ")}"))
    val modelHDFSToLocalPath = modelMeta.modelHDFSToLocalPath
    val f = (v: org.apache.spark.ml.linalg.Vector, modelPath: String) => {
      val modelRow = InternalRow.fromSeq(Seq(modelHDFSToLocalPath.getOrElse(modelPath, "")))
      val trainParamsRow = InternalRow.fromSeq(Seq(ArrayBasedMapData(trainParams)))
      val v_ser = pickleInternalRow(Seq(ser_vector(v)).toIterator, vector_schema())
      val v_ser2 = pickleInternalRow(Seq(modelRow).toIterator, StructType(Seq(StructField("modelPath", StringType))))
      var v_ser3 = v_ser ++ v_ser2
      if (enableCopyTrainParamsToPython) {
        val v_ser4 = pickleInternalRow(Seq(trainParamsRow).toIterator, StructType(Seq(StructField("trainParams", MapType(StringType, StringType)))))
        v_ser3 = v_ser3 ++ v_ser4
      }

      if (PredictTaskContext.get() == null) {
        PredictTaskContext.setTaskContext(APIDeployPythonRunnerEnv.createTaskContext())
      }

      val iter = WowPythonRunner.runner2(
        Option(daemonCommand), Option(workerCommand),
        command, envs,
        recordLog,
        SQLPythonAlg.isAPIService()
      ).run(
        v_ser3,
        PredictTaskContext.get().partitionId(),
        PredictTaskContext.get()
      )
      val res = ArrayBuffer[Array[Byte]]()
      while (iter.hasNext) {
        res += iter.next()
      }

      val predictValue = VectorSerDer.deser_vector(unpickle(res(0)).asInstanceOf[java.util.ArrayList[Object]].get(0))
      predictValue
    }

    val f2 = (v: org.apache.spark.ml.linalg.Vector) => {
      models.value.map { modelPath =>
        val resV = f(v, modelPath)
        (resV(resV.argmax), resV)
      }.sortBy(f => f._1).reverse.head._2
    }
    logInfo(format("Generate UDF in MSQL"))
    MLSQLUtils.createUserDefinedFunction(f2, VectorType, Some(Seq(VectorType)))
  }
}
