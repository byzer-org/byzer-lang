package streaming.dsl.mmlib.algs

import org.apache.spark.ml.param.Param
import org.apache.spark.ps.cluster.Message
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.common.HDFSOperator
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

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

    val remoteCommand = Message.CreateOrRemovePythonCondaEnv($(condaYamlFilePath), params, wowCommand)

    val executorNum = if (spark.sparkContext.isLocal) {
      val psDriverBackend = PlatformManager.getRuntime.asInstanceOf[SparkRuntime].localSchedulerBackend
      psDriverBackend.localEndpoint.askSync[Integer](remoteCommand)
    } else {
      val psDriverBackend = PlatformManager.getRuntime.asInstanceOf[SparkRuntime].psDriverBackend
      psDriverBackend.psDriverRpcEndpointRef.askSync[Integer](remoteCommand)
    }
    import spark.implicits._
    Seq[Seq[Int]](Seq(executorNum)).toDF("success_executor_num")
  }


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = throw new RuntimeException("register is not support")

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = throw new RuntimeException("register is not support")

  final val command: Param[String] = new Param[String](this, "command", "", isValid = (s:String) => {
    s == "create" || s == "remove"
  })

  final val condaYamlFilePath: Param[String] = new Param[String](this, "condaYamlFilePath", "")
  final val condaFile: Param[String] = new Param[String](this, "condaFile", "")
}
