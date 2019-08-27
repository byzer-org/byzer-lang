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

/**
  * Created by allwefantasy on 30/9/2018.
  */
class PythonAlgExecCommand(pythonScript: PythonScript,
                           mlflowConfig: Option[MLFlowConfig],
                           pythonConfig: Option[PythonConfig],
                           envs: Map[String, String]
                          ) {

  def generateCommand(commandType: String, envName: Option[String] = None) = {
    pythonScript.scriptType match {
      case MLFlow =>
        val project = MLProject.loadProject(pythonScript.filePath, envs)
        Seq("bash", "-c", project.entryPointCommandWithConda(commandType, envName))

      case _ =>

        envName match {
          case Some(name) =>
            Seq("bash", "-c", s"source activate ${name} && python ${pythonScript.fileName}")
          case None =>
            Seq(pythonConfig.map(_.pythonPath).getOrElse(
              throw new IllegalArgumentException("pythonPath should be configured"))) ++
              pythonConfig.map(_.pythonParam).getOrElse(Seq()) ++
              Seq(pythonScript.fileName)
        }


    }
  }


}





