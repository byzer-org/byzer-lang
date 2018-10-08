package streaming.dsl.mmlib.algs.python

import org.apache.spark.sql.SparkSession
import streaming.dsl.mmlib.algs.SQLPythonAlg
import streaming.log.{Logging, WowLog}

import scala.collection.JavaConverters._

class PythonLoad extends Logging with WowLog with Serializable {
  def load(sparkSession: SparkSession, _path: String, params: Map[String, String]): Any = {

    val modelMetaManager = new ModelMetaManager(sparkSession, _path, params)
    val modelMeta = modelMetaManager.loadMetaAndModel
    val localPathConfig = LocalPathConfig.buildFromParams(_path)

    val taskDirectory = localPathConfig.localRunPath + "/" + modelMeta.pythonScript.projectName

    var (selectedFitParam, resourceParams) = new ResourceManager(params).loadResourceInRegister(sparkSession, modelMeta)


    modelMeta.pythonScript.scriptType match {
      case MLFlow =>
        logInfo(format(s"'${modelMeta.pythonScript.projectName}' is MLflow project. download it from [${modelMeta.pythonScript.filePath}] to local [${taskDirectory}]"))
        SQLPythonAlg.distributePythonProject(sparkSession, taskDirectory, Option(modelMeta.pythonScript.filePath)).foreach(path => {
          resourceParams += ("mlFlowProjectPath" -> path)
        })

      case _ => None
    }
    val pythonProjectPath = params.get("pythonProjectPath")

    if (pythonProjectPath.isDefined) {
      logInfo(format(s"'${modelMeta.pythonScript.projectName}' is Normal project. download it from [${modelMeta.pythonScript.filePath}] to local [${taskDirectory}]"))
      SQLPythonAlg.distributePythonProject(sparkSession, taskDirectory, Option(modelMeta.pythonScript.filePath)).foreach(path => {
        resourceParams += ("pythonProjectPath" -> path)
      })
    }

    modelMeta.copy(resources = selectedFitParam + ("resource" -> resourceParams.asJava), taskDirectory = Option(taskDirectory))
  }
}
