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
import java.nio.charset.Charset
import java.util.UUID

import net.sf.json.JSONObject
import org.apache.commons.io.FileUtils
import streaming.common.HDFSOperator
import streaming.common.shell.ShellCommand
import streaming.log.Logging

import scala.collection.JavaConverters._


object BasicCondaEnvManager {
  val condaHomeKey = "MLFLOW_CONDA_HOME"
  val MLSQL_INSTNANCE_NAME_KEY = "mlsql_instance_name"
}

class BasicCondaEnvManager(options: Map[String, String]) extends Logging {

  def validateCondaExec = {
    val condaPath = getCondaBinExecutable("conda")
    try {
      ShellCommand.execCmd(s"${condaPath} --help")
    } catch {
      case e: Exception =>
        logError(s"Could not find Conda executable at ${condaPath}.", e)
        throw new RuntimeException(
          s"""
             |Could not find Conda executable at ${condaPath}.
             |Ensure Conda is installed as per the instructions
             |at https://conda.io/docs/user-guide/install/index.html. You can
             |also configure MLSQL to look for a specific Conda executable
             |by setting the MLFLOW_CONDA_HOME environment variable in where clause of  train/run statement to the path of the Conda
             |or configure it in environment.
             |
             |Here are how we get the conda home:
             |
             |def getCondaBinExecutable(executableName: String) = {
             |    val condaHome = options.get(BasicCondaEnvManager.condaHomeKey) match {
             |      case Some(home) => home
             |      case None => System.getenv(BasicCondaEnvManager.condaHomeKey)
             |    }
             |    if (condaHome != null) {
             |      s"$${condaHome}/bin/$${executableName}"
             |    } else executableName
             |  }
        """.stripMargin)
    }
    condaPath
  }

  def getOrCreateCondaEnv(condaEnvPath: Option[String]) = {

    val condaPath = validateCondaExec
    val stdout = ShellCommand.execCmd(s"${condaPath} env list --json")

    val envNames = JSONObject.fromObject(stdout).getJSONArray("envs").asScala.map(_.asInstanceOf[String].split("/").last).toSet
    val projectEnvName = getCondaEnvName(condaEnvPath)
    if (!envNames.contains(projectEnvName)) {
      logInfo(s"=== Creating conda environment $projectEnvName ===")
      condaEnvPath match {
        case Some(path) =>
          val tempFile = "/tmp/" + UUID.randomUUID() + ".yaml"
          try {
            FileUtils.write(new File(tempFile), getCondaYamlContent(condaEnvPath), Charset.forName("utf-8"))
            val command = s"${condaPath} env create -n $projectEnvName --file $tempFile"
            logInfo(s"=== ${command} ===")
            ShellCommand.execCmd(command)
          } catch {
            case e: Exception =>
              // try to remove the partial created env.
              try {
                removeEnv(condaEnvPath)
              } catch {
                case e1: Exception => // do nothing.
              }

              throw e
          }
          finally {
            FileUtils.forceDelete(new File(tempFile))
          }
        case None =>
      }
    }

    projectEnvName
  }

  def removeEnv(condaEnvPath: Option[String]) = {
    val condaPath = validateCondaExec
    val projectEnvName = getCondaEnvName(condaEnvPath)
    val command = s"${condaPath} env remove -y -n ${projectEnvName}"
    logInfo(command)
    ShellCommand.execCmd(s"${condaPath} env remove -y -n ${projectEnvName}")
  }

  def sha1(str: String) = {
    val md = java.security.MessageDigest.getInstance("SHA-1")
    val ha = md.digest(str.getBytes).map("%02x".format(_)).mkString
    ha
  }

  def getCondaEnvName(condaEnvPath: Option[String]) = {
    require(options.contains(BasicCondaEnvManager.MLSQL_INSTNANCE_NAME_KEY), s"${BasicCondaEnvManager.MLSQL_INSTNANCE_NAME_KEY} is required")
    val prefix = options(BasicCondaEnvManager.MLSQL_INSTNANCE_NAME_KEY)
    val condaEnvContents = condaEnvPath match {
      case Some(cep) =>
        // we should read from local ,but for now, we read from hdfs
        // scala.io.Source.fromFile(new File(cep)).getLines().mkString("\n")
        HDFSOperator.readFile(cep)
      case None => ""
    }
    s"mlflow-${prefix}-${sha1(condaEnvContents)}"
  }

  def getCondaYamlContent(condaEnvPath: Option[String]) = {
    val condaEnvContents = condaEnvPath match {
      case Some(cep) =>
        // we should read from local ,but for now, we read from hdfs
        // scala.io.Source.fromFile(new File(cep)).getLines().mkString("\n")
        HDFSOperator.readFile(cep)
      case None => ""
    }
    condaEnvContents
  }

  def getCondaBinExecutable(executableName: String) = {
    val condaHome = options.get(BasicCondaEnvManager.condaHomeKey) match {
      case Some(home) => home
      case None => System.getenv(BasicCondaEnvManager.condaHomeKey)
    }
    if (condaHome != null) {
      s"${condaHome}/bin/${executableName}"
    } else executableName
  }
}




