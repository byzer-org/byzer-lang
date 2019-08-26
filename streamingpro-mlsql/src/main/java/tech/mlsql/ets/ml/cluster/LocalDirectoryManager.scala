package tech.mlsql.ets.ml.cluster

import java.io.File

import org.apache.commons.io.FileUtils
import streaming.dsl.mmlib.algs.python.{LocalPathConfig, MLFlow, PythonScriptType}
import streaming.dsl.mmlib.algs.{SQLPythonAlg, SQLPythonFunc}
import tech.mlsql.common.utils.log.Logging

/**
  * 2019-08-26 WilliamZhu(allwefantasy@gmail.com)
  */
object LocalDirectoryManager extends Logging {
  def setUpTaskDirectory(projectName: String) = {
    val taskDirectory = SQLPythonFunc.getLocalRunPath(projectName)
    FileUtils.forceMkdir(new File(taskDirectory))
    // copy project file to taskDirectory
    taskDirectory
  }

  def localModelPath = {
    val localPathConfig = LocalPathConfig.buildFromParams(null)
    localPathConfig.localModelPath
  }

  def downloadProject(taskDirectory: String,
                      pythonProjectPath: Option[String],
                      projectType: PythonScriptType) = {
    pythonProjectPath match {
      case Some(_) =>
        if (projectType == MLFlow) {
          SQLPythonAlg.downloadPythonProject(taskDirectory, pythonProjectPath)
        } else {
          val localPath = taskDirectory + "/" + pythonProjectPath.get.split("/").last
          SQLPythonAlg.downloadPythonProject(localPath, pythonProjectPath)
        }


      case None =>
        // this will not happen, cause even script is a project contains only one python script file.
        throw new RuntimeException("The project or script you configured in pythonScriptPath is not a validate project")
    }
  }
}
