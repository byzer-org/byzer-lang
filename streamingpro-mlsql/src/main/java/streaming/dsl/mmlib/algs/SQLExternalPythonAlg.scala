package streaming.dsl.mmlib.algs

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession

/**
 * a extention of [[SQLPythonAlg]], which you can train python algthrim without mlsql.
 */
class SQLExternalPythonAlg extends SQLPythonAlg {

  override def load(sparkSession: SparkSession,
                    _path: String,
                    params: Map[String, String]): Any = {
    val systemParam = mapParams("systemParam", params)
    val pythonPath = systemParam.getOrElse("pythonPath", "python")
    val pythonVer = systemParam.getOrElse("pythonVer", "2.7")
    val metasTemp = Seq(Map("pythonPath" -> pythonPath, "pythonVer" -> pythonVer))

    val algIndex = params.getOrElse("algIndex", "-1").toInt
    val fitParam = arrayParamsWithIndex("fitParam", params)
    val selectedFitParam = {
      if (fitParam.size > 0) {
        fitParam(algIndex)._2
      } else {
        Map.empty[String, String]
      }
    }
    var resourceParams = Map.empty[String, String]


    // distribute resources
    val loadResource = selectedFitParam.keys.map(_.split("\\.")(0)).toSet.contains("resource")
    if (loadResource) {
      val resources = Functions.mapParams(s"resource", selectedFitParam)
      resources.foreach {
        case (resourceName, resourcePath) =>
          val tempResourceLocalPath = SQLPythonFunc.getLocalTempResourcePath(resourcePath, resourceName)
          resourceParams += (resourceName -> tempResourceLocalPath)
          distributeResource(sparkSession, resourcePath, tempResourceLocalPath)
      }
    }

    // distribute python project

    val loadPythonProject = params.contains("pythonProjectPath")
    if (loadPythonProject) {
      distributePythonProject(sparkSession,_path, params).foreach(path => {
        resourceParams += ("pythonProjectPath" -> path)
      })
    }

    (Seq(""), metasTemp, params, selectedFitParam + ("resource" -> resourceParams.asJava))
  }
}
