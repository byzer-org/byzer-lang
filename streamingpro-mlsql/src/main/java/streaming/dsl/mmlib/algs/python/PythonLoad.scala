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

package streaming.dsl.mmlib.algs.python

import org.apache.spark.sql.SparkSession
import streaming.dsl.mmlib.algs.SQLPythonAlg
import streaming.log.WowLog
import tech.mlsql.common.utils.log.Logging

import scala.collection.JavaConverters._

class PythonLoad extends Logging with WowLog with Serializable {
  def load(sparkSession: SparkSession, _path: String, params: Map[String, String]): ModelMeta = {

    val modelMetaManager = new ModelMetaManager(sparkSession, _path, params)
    val localPathConfig = LocalPathConfig.buildFromParams(_path)
    val modelMeta = modelMetaManager.loadMetaAndModel(localPathConfig, Map())

    val taskDirectory = localPathConfig.localRunPath + "/" + modelMeta.pythonScript.projectName

    var (selectedFitParam, resourceParams, modelHDFSToLocalPath) = new ResourceManager(params).loadResourceInRegister(sparkSession, modelMeta)


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

    modelMeta.copy(resources = selectedFitParam + ("resource" -> resourceParams.asJava), taskDirectory = Option(taskDirectory), modelHDFSToLocalPath = modelHDFSToLocalPath)
  }
}
