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

package streaming.test.pythonalg.code

case class ScriptCode(modelPath: String, projectPath: String, featureTablePath: String = "/tmp/featureTable")

object ScriptCode {


  val train =
    """
      |load csv.`${projectPath}/wine-quality.csv`
      |where header="true" and inferSchema="true"
      |as data;
      |
      |
      |
      |train data as PythonAlg.`${modelPath}`
      |
      | where pythonScriptPath="${projectPath}"
      | and keepVersion="true"
      | and  enableDataLocal="true"
      | and  dataLocalFormat="csv"
      | ${kv}
      |-- and  systemParam.envs='''{"MLFLOW_CONDA_HOME":"/anaconda3"}'''
      | ;
    """.stripMargin

  val processing =
    """
      |set feature_fun='''
      |
      |def apply(self,m):
      |    import json
      |    obj = json.loads(m)
      |    features = []
      |    for attribute, value in obj.items():
      |        if attribute != "quality":
      |            features.append(value)
      |    return features
      |''';
      |
      |load script.`feature_fun` as scriptTable;
      |
      |register ScriptUDF.`scriptTable` as featureFun options
      |and lang="python"
      |and dataType="array(double)"
      |;
      |
      |
      | select featureFun(to_json(struct(*))) as features from data  as featureTable;
    """.stripMargin


  val batchPredict =
    """
      | predict data as PythonAlg.`${modelPath}`;
    """.stripMargin

  val apiPredict =
    """
      | register PythonAlg.`${modelPath}` as pj;
    """.stripMargin
}
