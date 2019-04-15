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

package streaming.dsl.mmlib.algs

import org.apache.spark.sql.SparkSession
import streaming.dsl.mmlib.algs.python._

import scala.collection.JavaConverters._

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

    // distribute python project
    val localPathConfig = LocalPathConfig.buildFromParams(_path)

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


    val loadPythonProject = params.contains("pythonProjectPath")
    if (loadPythonProject) {
      val taskDirectory = localPathConfig.localRunPath
      SQLPythonAlg.distributePythonProject(sparkSession, taskDirectory, params.get("pythonProjectPath")).foreach(path => {
        resourceParams += ("pythonProjectPath" -> path)
      })
    }
    val taskDirectory = localPathConfig.localRunPath + params("pythonProjectPath").split("/").last
    val pythonScript = PythonAlgProject.loadProject(params, sparkSession)
    val resources = selectedFitParam + ("resource" -> resourceParams.asJava)
    ModelMeta(pythonScript.get, params, Seq(""), resources, localPathConfig, Map(), Option(taskDirectory))
  }
}
