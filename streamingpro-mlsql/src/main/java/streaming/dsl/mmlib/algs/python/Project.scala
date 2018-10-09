package streaming.dsl.mmlib.algs.python

import java.io.File
import java.nio.file.Paths
import java.util.UUID

import net.csdn.common.settings.{ImmutableSettings, Settings}
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import streaming.common.HDFSOperator
import streaming.common.shell.ShellCommand
import streaming.log.{Logging, WowLog}
import streaming.session.MLSQLException

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
            throw new MLSQLException(s"pythonScriptPath=$path should be a directory containing MLproject file or a python file.")
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

  def entryPointCommandWithConda(commandType: String) = {
    val condaEnvManager = new CondaEnvManager(options)
    val condaEnvName = condaEnvManager.getOrCreateCondaEnv(Option(projectDir + s"/${MLProject.DEFAULT_CONDA_ENV_NAME}"))
    val entryPointCommandWithConda = commandWithConda(
      condaEnvManager.getCondaBinExecutable("activate"),
      condaEnvName, commandType
    )
    logInfo(format(s"=== Running command '${entryPointCommandWithConda}' in run with ID '${UUID.randomUUID().toString}' === "))
    entryPointCommandWithConda
  }

  def condaEnvCommand = {
    val condaEnvManager = new CondaEnvManager(options)
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

object CondaEnvManager {
  val condaHomeKey = "MLFLOW_CONDA_HOME"
}

class CondaEnvManager(options: Map[String, String]) extends Logging with WowLog {

  def getOrCreateCondaEnv(condaEnvPath: Option[String]) = {
    val condaPath = getCondaBinExecutable("conda")
    try {
      ShellCommand.execCmd(s"${condaPath} --help")
    } catch {
      case e: Exception =>
        logError(s"Could not find Conda executable at ${condaPath}.", e)
        throw new MLSQLException(
          s"""
             |Could not find Conda executable at ${condaPath}.
             |Ensure Conda is installed as per the instructions
             |at https://conda.io/docs/user-guide/install/index.html. You can
             |also configure MLflow to look for a specific Conda executable
             |by setting the MLFLOW_CONDA_HOME environment variable to the path of the Conda
        """.stripMargin)
    }

    val stdout = ShellCommand.execCmd(s"${condaPath} env list --json")
    implicit val formats = DefaultFormats
    val envNames = (parse(stdout) \ "envs").extract[List[String]].map(_.split("/").last).toSet
    val projectEnvName = getCondaEnvName(condaEnvPath)
    if (!envNames.contains(projectEnvName)) {
      logInfo(format(s"=== Creating conda environment $projectEnvName ==="))
      condaEnvPath match {
        case Some(path) =>
          ShellCommand.execCmd(s"${condaPath} env create -n $projectEnvName --file ${path}")
        case None =>
          ShellCommand.execCmd(s"${condaPath} create  -n $projectEnvName python")
      }
    }

    projectEnvName
  }

  def sha1(str: String) = {
    val md = java.security.MessageDigest.getInstance("SHA-1")
    val ha = md.digest(str.getBytes).map("%02x".format(_)).mkString
    ha
  }

  def getCondaEnvName(condaEnvPath: Option[String]) = {
    val condaEnvContents = condaEnvPath match {
      case Some(cep) => scala.io.Source.fromFile(new File(cep)).getLines().mkString("\n")
      case None => ""
    }
    s"mlflow-${sha1(condaEnvContents)}"
  }

  def getCondaBinExecutable(executableName: String) = {
    val condaHome = options.get(CondaEnvManager.condaHomeKey) match {
      case Some(home) => home
      case None => System.getenv(CondaEnvManager.condaHomeKey)
    }
    if (condaHome != null) {
      s"${condaHome}/bin/${executableName}"
    } else executableName
  }
}


