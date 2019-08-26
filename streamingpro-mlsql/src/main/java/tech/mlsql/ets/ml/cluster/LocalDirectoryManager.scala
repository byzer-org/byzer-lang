package tech.mlsql.ets.ml.cluster

import java.io.File

import org.apache.commons.io.FileUtils
import streaming.dsl.mmlib.algs.SQLPythonFunc
import streaming.dsl.mmlib.algs.python.LocalPathConfig

/**
  * 2019-08-26 WilliamZhu(allwefantasy@gmail.com)
  */
object LocalDirectoryManager {
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
}
