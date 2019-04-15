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

  val _j1 =
    """
      |set python1='''
      |import os
      |import warnings
      |import sys
      |
      |import mlsql
      |
      |if __name__ == "__main__":
      |    warnings.filterwarnings("ignore")
      |
      |    tempDataLocalPath = mlsql.internal_system_param["tempDataLocalPath"]
      |
      |    isp = mlsql.params()["internalSystemParam"]
      |    tempModelLocalPath = isp["tempModelLocalPath"]
      |    if not os.path.exists(tempModelLocalPath):
      |        os.makedirs(tempModelLocalPath)
      |    with open(tempModelLocalPath + "/result.txt", "w") as f:
      |        f.write("jack")
      |''';
      |
      |set dependencies='''
      |name: tutorial
      |dependencies:
      |  - python=3.6
      |  - pip:
      |    - numpy==1.14.3
      |    - kafka-python==1.4.3
      |    - pyspark==2.3.2
      |    - pandas==0.22.0
      |    - scikit-learn==0.19.1
      |    - scipy==1.1.0
      |''';
      |
      |set modelPath="/tmp/jack2";
      |
      |set data='''
      |{"jack":1}
      |''';
      |
      |load jsonStr.`data` as testData;
      |load script.`python1` as python1;
      |load script.`dependencies` as dependencies;
      |run testData as PythonEnvExt.`/tmp/jack` where condaFile="dependencies" and command="create";
      |-- train sklearn model
      |run testData as PythonParallelExt.`${modelPath}`
      |where scripts="python1"
      |and entryPoint="python1"
      |and condaFile="dependencies"
      |;
      |
      |load text.`${modelPath}/model/0` as output;   -- 查看目标文件
      |
      |
    """.stripMargin

  val _j3 =
    """
      |set python1='''
      |import os
      |import warnings
      |import sys
      |import json
      |
      |import mlsql
      |
      |if __name__ == "__main__":
      |    warnings.filterwarnings("ignore")
      |
      |    tempDataLocalPath = mlsql.internal_system_param["tempDataLocalPath"]
      |
      |    isp = mlsql.params()["internalSystemParam"]
      |    tempModelLocalPath = isp["tempModelLocalPath"]
      |    dir_list_A = os.listdir(mlsql.internal_system_param["resource"]["a"])
      |    dir_list_B = os.listdir(mlsql.internal_system_param["resource"]["b"])
      |    if not os.path.exists(tempModelLocalPath):
      |        os.makedirs(tempModelLocalPath)
      |    with open(tempModelLocalPath + "/result.txt", "w") as f:
      |        f.write(json.dumps({"wow":"jack","resource_a":len(dir_list_A),"resource_b":len(dir_list_B)}))
      |''';
      |
      |set dependencies='''
      |name: tutorial
      |dependencies:
      |  - python=3.6
      |  - pip:
      |    - numpy==1.14.3
      |    - kafka-python==1.4.3
      |    - pyspark==2.3.2
      |    - pandas==0.22.0
      |    - scikit-learn==0.19.1
      |    - scipy==1.1.0
      |''';
      |
      |set modelPath="/tmp/jack2";
      |
      |set data='''
      |{"jack":1}
      |{"jack":2}
      |{"jack":3}
      |{"jack":4}
      |{"jack":5}
      |{"jack":6}
      |''';
      |
      |load jsonStr.`data` as testData;
      |load script.`python1` as python1;
      |load script.`dependencies` as dependencies;
      |
      |run testData as PythonEnvExt.`/tmp/jack` where condaFile="dependencies" and command="create";
      |
      |run testData as RepartitionExt.`` where partitionNum="3" as testData1;
      |-- train sklearn model
      |run testData1 as PythonParallelExt.`${modelPath}`
      |where scripts="python1"
      |and entryPoint="python1"
      |and condaFile="dependencies"
      |and partitionKey="hp_date"
      |and fitParam.0.resource.a="/tmp/resource"
      |and fitParam.resource.b="/tmp/resource"
      |;
      |
      |load json.`${modelPath}/model/` as output;   -- 查看目标文件
      |
      |
    """.stripMargin


  val _j2 =
    """
      |set python1='''
      |import os
      |import warnings
      |import sys
      |
      |import mlsql
      |
      |if __name__ == "__main__":
      |    warnings.filterwarnings("ignore")
      |
      |    tempDataLocalPath = mlsql.internal_system_param["tempDataLocalPath"]
      |
      |    isp = mlsql.params()["internalSystemParam"]
      |    tempModelLocalPath = isp["tempModelLocalPath"]
      |    if not os.path.exists(tempModelLocalPath):
      |        os.makedirs(tempModelLocalPath)
      |    with open(tempModelLocalPath + "/result.txt", "w") as f:
      |        f.write("jack")
      |''';
      |
      |set dependencies='''
      |name: tutorial
      |dependencies:
      |  - python=3.6
      |  - pip:
      |    - numpy==1.14.3
      |    - kafka-python==1.4.3
      |    - pyspark==2.3.2
      |    - pandas==0.22.0
      |    - scikit-learn==0.19.1
      |    - scipy==1.1.0
      |''';
      |
      |set modelPath="/tmp/jack2";
      |
      |set data='''
      |{"jack":1}
      |{"jack":2}
      |{"jack":3}
      |{"jack":4}
      |{"jack":5}
      |{"jack":6}
      |''';
      |
      |load jsonStr.`data` as testData;
      |load script.`python1` as python1;
      |load script.`dependencies` as dependencies;
      |run testData as PythonEnvExt.`/tmp/jack` where condaFile="dependencies" and command="create";
      |run testData as RepartitionExt.`` where partitionNum="3" as testData1;
      |-- train sklearn model
      |run testData1 as PythonAlg.`${modelPath}`
      |where scripts="python1"
      |and entryPoint="python1"
      |and condaFile="dependencies"
      |and fitParam.0.abc="test"
      |and keepVersion="false"
      |;
      |
      |load text.`${modelPath}/model/0` as output;   -- 查看目标文件
      |
      |
    """.stripMargin

  val _j2_PREDICT =
    """
      |set python1='''
      |import os
      |import warnings
      |import sys
      |
      |import mlsql
      |
      |if __name__ == "__main__":
      |    warnings.filterwarnings("ignore")
      |
      |    tempDataLocalPath = mlsql.internal_system_param["tempDataLocalPath"]
      |
      |    isp = mlsql.params()["internalSystemParam"]
      |    tempModelLocalPath = isp["tempModelLocalPath"]
      |    if not os.path.exists(tempModelLocalPath):
      |        os.makedirs(tempModelLocalPath)
      |    with open(tempModelLocalPath + "/result.txt", "w") as f:
      |        f.write("jack")
      |''';
      |
      |set dependencies='''
      |name: tutorial
      |dependencies:
      |  - python=3.6
      |  - pip:
      |    - numpy==1.14.3
      |    - kafka-python==1.4.3
      |    - pyspark==2.3.2
      |    - pandas==0.22.0
      |    - scikit-learn==0.19.1
      |    - scipy==1.1.0
      |''';
      |
      |set modelPath="/tmp/jack2";
      |
      |set data='''
      |{"jack":1}
      |{"jack":2}
      |{"jack":3}
      |{"jack":4}
      |{"jack":5}
      |{"jack":6}
      |''';
      |
      |load jsonStr.`data` as testData;
      |load script.`python1` as python1;
      |load script.`dependencies` as dependencies;
      |run testData as PythonEnvExt.`/tmp/jack` where condaFile="dependencies" and command="create";
      |
      |register PythonAlg.`${modelPath}` as  wowPredict where entryPoint="python1";
      |
    """.stripMargin

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
      | and fitParam.0.abc="example"
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
