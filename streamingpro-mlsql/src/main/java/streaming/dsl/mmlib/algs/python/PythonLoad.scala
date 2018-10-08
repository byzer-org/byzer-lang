package streaming.dsl.mmlib.algs.python

import org.apache.spark.sql.SparkSession
import streaming.dsl.mmlib.algs.SQLPythonAlg
import scala.collection.JavaConverters._

class PythonLoad extends Serializable {
  def load(sparkSession: SparkSession, _path: String, params: Map[String, String]): Any = {

    val modelMetaManager = new ModelMetaManager(sparkSession, _path, params)
    val modelMeta = modelMetaManager.loadMetaAndModel
    val localPathConfig = LocalPathConfig.buildFromParams(_path)

    val taskDirectory = localPathConfig.localRunPath + "/" + modelMeta.pythonScript.projectName

    var (selectedFitParam, resourceParams) = new ResourceManager(params).loadResourceInRegister(sparkSession, modelMeta)


    modelMeta.pythonScript.scriptType match {
      case MLFlow =>
        SQLPythonAlg.distributePythonProject(sparkSession, taskDirectory, Option(modelMeta.pythonScript.filePath)).foreach(path => {
          resourceParams += ("mlFlowProjectPath" -> path)
        })

      case _ => None
    }
    val pythonProjectPath = params.get("pythonProjectPath")

    if (pythonProjectPath.isDefined) {
      SQLPythonAlg.distributePythonProject(sparkSession, taskDirectory, Option(modelMeta.pythonScript.filePath)).foreach(path => {
        resourceParams += ("pythonProjectPath" -> path)
      })
    }

    modelMeta.copy(resources = selectedFitParam + ("resource" -> resourceParams.asJava), taskDirectory = Option(taskDirectory))
  }
}
