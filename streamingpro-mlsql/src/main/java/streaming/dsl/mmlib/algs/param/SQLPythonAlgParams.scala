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

package streaming.dsl.mmlib.algs.param

import org.apache.spark.ml.param.{BooleanParam, Param}
import org.apache.spark.sql.DataFrame
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.python.{AutoCreateMLproject, PythonAlgProject}

/**
  * Created by allwefantasy on 28/9/2018.
  */
trait SQLPythonAlgParams extends BaseParams with Functions {

  final val enableDataLocal: BooleanParam = new BooleanParam(this, "enableDataLocal",
    "Save prepared data to HDFS and then copy them to local")
  setDefault(enableDataLocal, true)


  final val dataLocalFormat: Param[String] = new Param[String](this, "dataLocalFormat",
    "dataLocalFormat")
  setDefault(dataLocalFormat, "json")


  final val pythonScriptPath: Param[String] = new Param[String](this, "pythonScriptPath",
    "The location of python script")

  final val kafkaParam_bootstrap_servers: Param[String] = new Param[String](this, "kafkaParam.bootstrap.servers",
    "Set kafka server address")


  final val systemParam_pythonPath: Param[String] = new Param[String](this, "systemParam.pythonPath",
    "Configure python path")

  final val systemParam_pythonParam: Param[String] = new Param[String](this, "systemParam.pythonParam",
    "python params e.g -u")

  final val systemParam_pythonVer: Param[String] = new Param[String](this, "systemParam.pythonVer",
    "Configure python path")

  final val fitParam: Param[String] = new Param[String](this, "fitParam",
    "fitParam is dynamic params. e.g. fitParam.0.moduleName,fitParam.1.moduleName`")

  final val scripts: Param[String] = new Param[String](this, "scripts",
    "")

  final val projectPath: Param[String] = new Param(this, "projectPath",
    "")

  final val entryPoint: Param[String] = new Param(this, "entryPoint",
    "")

  final val batchPredictEntryPoint: Param[String] = new Param(this, "batchPredictEntryPoint",
    "")

  final val apiPredictEntryPoint: Param[String] = new Param(this, "apiPredictEntryPoint",
    "")

  final val condaFile: Param[String] = new Param(this, "condaFile",
    "")


  def autoConfigureAutoCreateProjectParams(params: Map[String, String]) = {

    params.get(scripts.name).map { item =>
      set(scripts, item)
      item
    }.getOrElse {
    }

    params.get(entryPoint.name).map { item =>
      set(entryPoint, item)
      item
    }.getOrElse {
    }

    params.get(batchPredictEntryPoint.name).map { item =>
      set(batchPredictEntryPoint, item)
      item
    }.getOrElse {
      set(batchPredictEntryPoint, "py_batch_predict")
    }

    params.get(apiPredictEntryPoint.name).map { item =>
      set(apiPredictEntryPoint, item)
      item
    }.getOrElse {
      set(apiPredictEntryPoint, "py_predict")
    }

    params.get(condaFile.name).map { item =>
      set(condaFile, item)
      item
    }.getOrElse {

    }
  }

  def setupProject(df: DataFrame, path: String, _params: Map[String, String]) = {
    pythonCheckRequirements(df)
    autoConfigureAutoCreateProjectParams(_params)
    var newParams = _params
    if (get(scripts).isDefined) {
      val autoCreateMLproject = new AutoCreateMLproject($(scripts), $(condaFile), $(entryPoint), $(batchPredictEntryPoint), $(apiPredictEntryPoint))
      val projectPath = autoCreateMLproject.saveProject(df.sparkSession, path)

      newParams = _params
      newParams += ("enableDataLocal" -> "true")
      newParams += ("pythonScriptPath" -> projectPath)
      newParams += ("pythonDescPath" -> projectPath)
    }
    val pythonProject = PythonAlgProject.loadProject(newParams, df.sparkSession)
    (pythonProject.get, newParams)
  }

}
