package streaming.dsl.mmlib.algs.python

import org.apache.spark.sql.SparkSession
import streaming.common.HDFSOperator
import streaming.dsl.mmlib.algs.{Functions, SQLPythonAlg, SQLPythonFunc}
import streaming.log.{Logging, WowLog}

class ResourceManager(params: Map[String, String]) extends Logging with WowLog {
  def loadResourceInTrain = {
    var resourceParams = Map.empty[String, String]
    if (params.keys.map(_.split("\\.")(0)).toSet.contains("resource")) {
      val resources = Functions.mapParams(s"resource", params)
      resources.foreach {
        case (resourceName, resourcePath) =>
          val tempResourceLocalPath = SQLPythonFunc.getLocalTempResourcePath(resourcePath, resourceName)
          var msg = s"resource paramter found,system will load resource ${resourcePath} in ${tempResourceLocalPath} in executor."
          logInfo(format(msg))
          HDFSOperator.copyToLocalFile(tempResourceLocalPath, resourcePath, true)
          resourceParams += (resourceName -> tempResourceLocalPath)
          msg = s"resource loaded."
          logInfo(format(msg))

      }
    }
    resourceParams
  }

  def loadResourceInRegister(sparkSession: SparkSession, modelMeta: ModelMeta) = {
    val algIndex = params.getOrElse("algIndex", "-1").toInt
    // load resource
    val fitParam = SQLPythonAlg.arrayParamsWithIndex("fitParam", modelMeta.trainParams)
    val selectedFitParam = if (algIndex == -1) Map[String, String]() else fitParam(algIndex)._2
    val loadResource = selectedFitParam.keys.map(_.split("\\.")(0)).toSet.contains("resource")
    var resourceParams = Map.empty[String, String]

    // make sure every executor have the model in local directory.
    // we should unregister manually
    modelMeta.modelEntityPaths.foreach { modelPath =>
      val tempModelLocalPath = SQLPythonFunc.getLocalTempModelPath(modelPath)
      SQLPythonAlg.distributeResource(sparkSession, modelPath, tempModelLocalPath)

      if (loadResource) {
        val resources = Functions.mapParams(s"resource", selectedFitParam)
        resources.foreach {
          case (resourceName, resourcePath) =>
            val tempResourceLocalPath = SQLPythonFunc.getLocalTempResourcePath(resourcePath, resourceName)
            resourceParams += (resourceName -> tempResourceLocalPath)
            SQLPythonAlg.distributeResource(sparkSession, resourcePath, tempResourceLocalPath)
        }
      }

    }
    (selectedFitParam,resourceParams)
  }
}
