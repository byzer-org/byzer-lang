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

import java.nio.file.Paths
import java.util.UUID

import net.csdn.common.settings.{ImmutableSettings, Settings}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.mlsql.session.MLSQLException
import streaming.log.WowLog
import tech.mlsql.common.utils.env.python.BasicCondaEnvManager
import tech.mlsql.common.utils.hdfs.HDFSOperator
import tech.mlsql.common.utils.log.Logging

object PythonAlgProject extends Logging with WowLog {

  def getPythonScriptPath(params: Map[String, String]) = {
    if (params.contains("pythonDescPath") || params.contains("pythonScriptPath")) {
      Some(params.getOrElse("pythonDescPath", params.getOrElse("pythonScriptPath", "")))
    } else None
  }

  def loadProject(params: Map[String, String], spark: SparkSession) = {

    getPythonScriptPath(params) match {
      case Some(path) =>
        if (HDFSOperator.isDir(path) && HDFSOperator.fileExists(Paths.get(path, "MLproject").toString)) {
          val project = path.split("/").last
          Some(PythonScript("", "", path, project, MLFlow))

        } else {
          if (!HDFSOperator.isFile(path)) {
            throw new MLSQLException(s"pythonScriptPath=$path should be a directory which contains MLproject file " +
              s"or directly a python file.")
          }
          val pythonScriptFileName = path.split("/").last
          val pythonScriptContent = spark.sparkContext.textFile(path, 1).collect().mkString("\n")
          Some(PythonScript(pythonScriptFileName, pythonScriptContent, path, UUID.randomUUID().toString, NormalProject))
        }

      case None => None
    }
  }
}


class MLProject(val projectDir: String, project: Settings, options: Map[String, String]) extends Logging with WowLog {

  private[this] def conda_env = {
    project.get(MLProject.conda_env)
  }

  private[this] def command(name: String) = {
    project.get(name)
  }

  private[this] def commandWithConda(activatePath: String, condaEnvName: String, commandType: String) = {
    s"source ${activatePath} ${condaEnvName} && ${command(commandType)}"
  }

  def entryPointCommandWithConda(commandType: String, envName: Option[String] = None) = {
    val condaEnvManager = new BasicCondaEnvManager(options)
    val condaEnvName = envName.getOrElse(condaEnvManager.getOrCreateCondaEnv(Option(projectDir + s"/${MLProject.DEFAULT_CONDA_ENV_NAME}")))
    val entryPointCommandWithConda = commandWithConda(
      condaEnvManager.getCondaBinExecutable("activate"),
      condaEnvName, commandType
    )
    logInfo(format(s"=== Running command '${entryPointCommandWithConda}' in run with ID '${UUID.randomUUID().toString}' === "))
    entryPointCommandWithConda
  }

  def condaEnvCommand = {
    val condaEnvManager = new BasicCondaEnvManager(options)
    val condaEnvName = condaEnvManager.getOrCreateCondaEnv(Option(projectDir + s"/${MLProject.DEFAULT_CONDA_ENV_NAME}"))
    val command = s"source ${condaEnvManager.getCondaBinExecutable("activate")} ${condaEnvName}"
    logInfo(format(s"=== generate command  '${command}' for ${projectDir} === "))
    command
  }

}

object MLProject {
  val name = "name"
  val conda_env = "conda_env"
  val train_command = "entry_points.main.train.command"
  val train_parameters = "entry_points.main.train.parameters"

  val api_predict_command = "entry_points.main.api_predict.command"
  val api_predict_parameters = "entry_points.main.api_predict.parameters"

  val batch_predict_command = "entry_points.main.batch_predict.command"
  val batch_predict_parameters = "entry_points.main.batch_predict.parameters"

  val DEFAULT_CONDA_ENV_NAME = "conda.yaml"
  val MLPROJECT = "MLproject"

  def loadProject(projectDir: String, options: Map[String, String]) = {
    val projectContent = HDFSOperator.readFile(projectDir + s"/${MLPROJECT}")
    val projectDesc = ImmutableSettings.settingsBuilder().loadFromSource(projectContent).build()
    new MLProject(projectDir, projectDesc, options)
  }
}

class AutoCreateMLproject(scripts: String, condaFile: String, entryPoint: String, batchPredictEntryPoint: String = "py_batch_predict", apiPredictEntryPoint: String = "py_predict") {

  def projectName = "mlsql-python-project"

  /*
     We will automatically create project for user according the configuration
   */
  def saveProject(sparkSession: SparkSession, path: String) = {
    val projectPath = path + s"/${projectName}"
    scripts.split(",").foreach { script =>
      val content = sparkSession.table(script).head().getString(0)
      HDFSOperator.saveFile(projectPath, script + ".py", Seq(("", content)).iterator)
    }
    HDFSOperator.saveFile(projectPath, "MLproject", Seq(("", MLprojectTemplate)).iterator)
    val condaContent = sparkSession.table(condaFile).head().getString(0)
    HDFSOperator.saveFile(projectPath, "conda.yaml", Seq(("", condaContent)).iterator)
    projectPath
  }


  private def MLprojectTemplate = {
    s"""
       |name: mlsql-python
       |
       |conda_env: conda.yaml
       |
       |entry_points:
       |  main:
       |    train:
       |        command: "python ${entryPoint}.py"
       |    batch_predict:
       |        command: "python ${batchPredictEntryPoint}.py"
       |    api_predict:
       |        command: "python ${apiPredictEntryPoint}.py"
     """.stripMargin
  }
}


