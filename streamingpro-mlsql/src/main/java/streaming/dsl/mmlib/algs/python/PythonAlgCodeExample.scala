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

import streaming.dsl.mmlib.{Code, SQLCode}

object PythonAlgCodeExample {
  def codeExample = {
    Code(SQLCode,
      """
        |-- you can find example project sklearn_elasticnet_wine in
        |-- [streamingpro_home]/examples
        |
        |set projectHome="/Users/allwefantasy/CSDNWorkSpace/mlflow/examples/sklearn_elasticnet_wine";
        |
        |load csv.`${projectHome}/wine-quality.csv`
        |where header="true" and inferSchema="true"
        |as data;
        |
        |-- train and generate a model in location `/tmp/abc`
        |train data as PythonAlg.`/tmp/abc`
        | where pythonScriptPath="${projectHome}"
        | and  dataLocalFormat="csv"
        | -- and systemParam.envs='''{"MLFLOW_CONDA_HOME":"/anaconda3"}'''
        | ;
        |
        |-- use predict statement to use model generated in /tmp/abc  predict the data
        |predict data as PythonAlg.`/tmp/abc`;
        |
        |-- if you wanna deploy the mode in API Server, just run StreamingPro
        |-- as local mode with `streaming.deploy.rest.api=true` configured.
        |-- then execute http request following:
        |
        |register PythonAlg.`/tmp/abc` as pj;
        |
        |-- then you can  request the API server like this:
        |
        |/**
        |curl code:
        |
        |curl -XPOST 'http://127.0.0.1:9003/model/predict' -d '
        |sql=select pj(vec_dense(features)) as p1
        |&data=[ {"features": [0.045, 8.8, 1.001, 45.0, 7.0, 170.0, 0.27, 0.45, 0.36, 3.0, 20.7]}]
        |&dataType=row
        |'
        |or use scala code:
        |
        |import org.apache.http.client.fluent.{Form, Request}
        |import org.apache.spark.graphx.VertexId
        |
        |object Test {
        |  def main(args: Array[String]): Unit = {
        |    val sql = "select pj(vec_dense(features)) as p1 "
        |
        |    val res = Request.Post("http://127.0.0.1:9003/model/predict").bodyForm(Form.form().
        |      add("sql", sql).
        |      add("data", "[ {"features": [0.045, 8.8, 1.001, 45.0, 7.0, 170.0, 0.27, 0.45, 0.36, 3.0, 20.7]}]").
        |      add("dataType", "row")
        |      .build()).execute().returnContent().asString()
        |    println(res)
        |  }
        |}
        |**/
        |
      """.stripMargin)
  }
}
