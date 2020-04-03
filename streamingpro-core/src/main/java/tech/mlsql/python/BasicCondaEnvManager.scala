package tech.mlsql.python

import java.io.File
import java.nio.charset.Charset
import java.util.UUID

import net.sf.json.JSONObject
import org.apache.commons.io.FileUtils
import tech.mlsql.common.utils.hdfs.HDFSOperator
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.shell.ShellCommand
import tech.mlsql.log.WriteLog

import scala.collection.JavaConverters._


object BasicCondaEnvManager {
  val condaHomeKey = "MLFLOW_CONDA_HOME"
  val MLSQL_INSTNANCE_NAME_KEY = "mlsql_instance_name"
}

class BasicCondaEnvManager(user: String, groupId: String, executorHostAndPort: String, options: Map[String, String]) extends Logging {

  def validateCondaExec = {
    val condaPath = getCondaBinExecutable("conda")
    try {
      val cr = ShellCommand.execCmdV2(condaPath, "--help")
      if (cr.exitCode != 0) {
        throw new RuntimeException(s"Fail to execute ${condaPath} --help command，out:${cr.out.string} error:${cr.err.string}")
      }
    } catch {
      case e: Exception =>
        logError(s"Could not find Conda executable at ${condaPath}.", e)
        throw new RuntimeException(
          s"""
             |${e.getMessage}
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
    val res = ShellCommand.execCmdV2(condaPath, "env", "list", "--json")
    if (res.exitCode != 0) {
      throw new RuntimeException(s"Fail to list env ，error:${res.err.string}")
    }
    val stdout = res.out.string

    val envNames = JSONObject.fromObject(stdout).getJSONArray("envs").asScala.map(_.asInstanceOf[String].split("/").last).toSet
    val projectEnvName = getCondaEnvName(condaEnvPath)
    if (!envNames.contains(projectEnvName)) {
      logInfo(s"=== Creating conda environment $projectEnvName ===")
      condaEnvPath match {
        case Some(path) =>
          val tempFile = "/tmp/" + UUID.randomUUID() + ".yaml"
          try {
            FileUtils.write(new File(tempFile), getCondaYamlContent(condaEnvPath), Charset.forName("utf-8"))
            val cr = ShellCommand.execCmdV2WithProcessing((s) => {
              println(s"Creating conda env:${projectEnvName}: " + s)
              WriteLog.write(List(s"Creating conda env in ${executorHostAndPort}: ${s}").iterator, Map("PY_EXECUTE_USER" -> user, "groupId" -> groupId))
            },
              condaPath, "env", "create", "-n", projectEnvName, "--file", tempFile)
            if (cr.exitCode != 0) {
              throw new RuntimeException(s"Fail to create env ${projectEnvName}，error:${cr.stderr.lines.mkString("\n")}")
            }
          } catch {
            case e: Exception =>
              // make sure once exception happen, we can remove
              // the partial created env.
              removeEnv(condaEnvPath)
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
    logInfo(s"${condaPath} env remove -y -n ${projectEnvName}")
    try {
      val cr = ShellCommand.execCmdV2(condaPath, "env", "remove", "-y", "-n", projectEnvName)
      if (cr.exitCode != 0) {
        throw new RuntimeException(s"Fail to remove env ${projectEnvName}，out:${cr.out.string} error:${cr.err.string}")
      }
      cr.out.string
    } catch {
      case e: Exception =>
        logError(s"Fail to remove env ${projectEnvName}", e)
        s"Fail to remove env ${projectEnvName}"
    }

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
