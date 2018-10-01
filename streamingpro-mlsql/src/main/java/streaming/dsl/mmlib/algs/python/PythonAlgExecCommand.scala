package streaming.dsl.mmlib.algs.python

import java.io.File
import java.util.UUID

import net.csdn.common.settings.{ImmutableSettings, Settings}
import streaming.common.shell.ShellCommand
import streaming.log.{Logging, WowLog}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import streaming.dsl.ScriptSQLExec

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 30/9/2018.
  */
class PythonAlgExecCommand(pythonScript: PythonScript,
                           mlflowConfig: Option[MLFlowConfig],
                           pythonConfig: Option[PythonConfig]) {

  def generateCommand = {
    pythonScript.scriptType match {
      case MLFlow =>
        val project = MLProject.loadProject(pythonScript.filePath)
        Seq("bash", "-c", project.entryPointCommandWithConda)

      case _ =>
        Seq(pythonConfig.map(_.pythonPath).getOrElse(
          throw new IllegalArgumentException("pythonPath should be configured"))) ++
          pythonConfig.map(_.pythonParam).getOrElse(Seq()) ++
          Seq(pythonScript.fileName)
    }
  }


}

class MLProject(val projectDir: String, project: Settings) extends Logging with WowLog {


  private[this] def parameters = {

    val parameterMap = project.getGroups(MLProject.parameters)
    parameterMap.map { f =>
      (f._1, f._2.getAsMap.toMap)
    }
  }

  private[this] def conda_env = {
    project.get(MLProject.conda_env)
  }

  private[this] def command = {
    project.get(MLProject.command)
  }

  private[this] def commandWithConda(activatePath: String, condaEnvName: String) = {
    s"source ${activatePath} ${condaEnvName} && ${command}"
  }

  def entryPointCommandWithConda = {
    val condaEnvManager = new CondaEnvManager()
    val condaEnvName = condaEnvManager.getOrCreateCondaEnv(Option(projectDir + s"/${MLProject.DEFAULT_CONDA_ENV_NAME}"))
    val entryPointCommandWithConda = commandWithConda(
      condaEnvManager.getCondaBinExecutable("activate"),
      condaEnvName
    )
    logInfo(format(s"=== Running command '${entryPointCommandWithConda}' in run with ID '${UUID.randomUUID().toString}' === "))
    entryPointCommandWithConda
  }

}

object MLProject {
  val name = "name"
  val conda_env = "conda_env"
  val command = "entry_points.main.command"
  val parameters = "entry_points.main.parameters"
  val DEFAULT_CONDA_ENV_NAME = "conda.yaml"
  val MLPROJECT = "MLproject"

  def loadProject(projectDir: String) = {
    val projectContent = scala.io.Source.fromFile(new File(projectDir + s"/${MLPROJECT}")).getLines().mkString("\n")
    val projectDesc = ImmutableSettings.settingsBuilder().loadFromSource(projectContent).build()
    new MLProject(projectDir, projectDesc)
  }
}

class CondaEnvManager extends Logging with WowLog {
  def getOrCreateCondaEnv(condaEnvPath: Option[String]) = {
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
    val condaHome = System.getenv("MLFLOW_CONDA_HOME")
    if (condaHome != null) {
      s"${condaHome}/bin/${executableName}"
    } else executableName
  }
}

object CondaEnvManager {
  def main(args: Array[String]): Unit = {
    val mlsqlContext = ScriptSQLExec.contextGetOrForTest()
    ScriptSQLExec.setContext(mlsqlContext)
    val project = MLProject.loadProject("/Users/allwefantasy/CSDNWorkSpace/mlflow/examples/sklearn_elasticnet_wine")
    //val condaEnvManager = new CondaEnvManager()
    //condaEnvManager.getOrCreateCondaEnv(Option(project.projectDir + "/conda.yaml"))

  }
}



