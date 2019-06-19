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

import org.apache.spark.MLSQLConf
import org.apache.spark.ml.param.Param
import org.apache.spark.ps.cluster.Message
import org.apache.spark.ps.cluster.Message.CreateOrRemovePythonCondaEnvResponse
import org.apache.spark.scheduler.cluster.PSDriverEndpoint
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.common.HDFSOperator
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.dsl.mmlib.algs.python.BasicCondaEnvManager

/**
  * 2019-01-16 WilliamZhu(allwefantasy@gmail.com)
  */
class SQLPythonEnvExt(override val uid: String) extends SQLAlg with WowParams {

  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    val spark = df.sparkSession

    params.get(command.name).map { s =>
      set(command, s)
      s
    }.getOrElse {
      throw new MLSQLException(s"${command.name} is required")
    }

    params.get(condaYamlFilePath.name).map { s =>
      set(condaYamlFilePath, s)
    }.getOrElse {
      params.get(condaFile.name).map { s =>
        val condaContent = spark.table(s).head().getString(0)
        val baseFile = path + "/__mlsql_temp_dir__/conda"
        val fileName = "conda.yaml"
        HDFSOperator.saveFile(baseFile, fileName, Seq(("", condaContent)).iterator)
        set(condaYamlFilePath, baseFile + "/" + fileName)
      }.getOrElse {
        throw new MLSQLException(s"${condaFile.name} || ${condaYamlFilePath} is required")
      }

    }

    val wowCommand = $(command) match {
      case "create" => Message.AddEnvCommand
      case "remove" => Message.RemoveEnvCommand
    }
    val appName = spark.sparkContext.getConf.get("spark.app.name")
    val remoteCommand = Message.CreateOrRemovePythonCondaEnv($(condaYamlFilePath), params ++ Map(BasicCondaEnvManager.MLSQL_INSTNANCE_NAME_KEY -> appName), wowCommand)

    val response = if (spark.sparkContext.isLocal) {
      val psDriverBackend = PlatformManager.getRuntime.asInstanceOf[SparkRuntime].localSchedulerBackend
      psDriverBackend.localEndpoint.askSync[CreateOrRemovePythonCondaEnvResponse](remoteCommand, PSDriverEndpoint.MLSQL_DEFAULT_RPC_TIMEOUT(spark.sparkContext.getConf))
    } else {
      val runtime = PlatformManager.getRuntime.asInstanceOf[SparkRuntime]
      assert(MLSQLConf.MLSQL_CLUSTER_PS_ENABLE.readFrom(runtime.configReader),
        """
          |Please make sure `-streaming.ps.cluster.enable ` set true and
          |make the mlsql main jar available in executor classpath. You can do like this(yarn mode):
          |
          |--jars ./libs/mlsql-spark_2.x-x.x.x.jar
          |--conf "spark.executor.extraClassPath=mlsql-spark_2.x-x.x.x.jar"
          |
          |If you deploy as Standalone mode, please send the  mlsql-spark_2.x-x.x.x.jar to every node, and configure like
          |following:
          |
          |--conf "spark.executor.extraClassPath= THE FULL PATH OF mlsql-spark_2.x-x.x.x.jar"
          |
        """.stripMargin)
      val psDriverBackend = runtime.psDriverBackend
      psDriverBackend.psDriverRpcEndpointRef.askSync[CreateOrRemovePythonCondaEnvResponse](remoteCommand, PSDriverEndpoint.MLSQL_DEFAULT_RPC_TIMEOUT(spark.sparkContext.getConf))
    }
    import spark.implicits._
    spark.createDataset[CreateOrRemovePythonCondaEnvResponse](Seq(response)).toDF()
  }


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = throw new RuntimeException("register is not support")

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = throw new RuntimeException("register is not support")

  final val command: Param[String] = new Param[String](this, "command", "create|remove", isValid = (s: String) => {
    s == "create" || s == "remove"
  })

  final val condaYamlFilePath: Param[String] = new Param[String](this, "condaYamlFilePath", "the conda file path")
  final val condaFile: Param[String] = new Param[String](this, "condaFile", "variable ref configured by set")

  override def explainParams(sparkSession: SparkSession): DataFrame = _explainParams(sparkSession)

  override def codeExample: Code = Code(SQLCode,
    """
      |```sql
      |set dependencies='''
      |name: tutorial4
      |dependencies:
      |  - python=3.6
      |  - pip
      |  - pip:
      |    - --index-url https://mirrors.aliyun.com/pypi/simple/
      |    - numpy==1.14.3
      |    - kafka==1.3.5
      |    - pyspark==2.3.2
      |    - pandas==0.22.0
      |''';
      |
      |load script.`dependencies` as dependencies;
      |run command as PythonEnvExt.`/tmp/jack` where condaFile="dependencies" and command="create";
      |```
      |
    """.stripMargin)
}
