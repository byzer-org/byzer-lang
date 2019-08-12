package streaming.dsl.mmlib.algs

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.core.datasource.DataSourceRepository
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.log.WowLog
import tech.mlsql.common.utils.log.Logging

/**
  * 2019-01-14 WilliamZhu(allwefantasy@gmail.com)
  */
class SQLDataSourceExt(override val uid: String) extends SQLAlg with WowParams with Logging with WowLog {


  override def skipPathPrefix: Boolean = true

  // path: es
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    params.get(command.name).map { item =>
      set(command, item)
      item
    }.getOrElse {
      set(command, "list")
    }

    params.get(repository.name).map { item =>
      set(repository, item)
      item
    }

    params.get(sparkV.name).map { item =>
      set(sparkV, item)
      item
    }.getOrElse {
      throw new MLSQLException("please set spark version")
    }

    params.get(scalaV.name).map { item =>
      set(scalaV, item)
      item
    }.getOrElse {
      set(scalaV, "2.11")
    }

    val rep = if (isDefined(repository)) $(repository) else ""
    val dataSourceRepository = new DataSourceRepository(rep)

    val spark = df.sparkSession
    import spark.implicits._
    $(command) match {
      case "list" => spark.createDataset[String](dataSourceRepository.list()).toDF("value")
      case "version" =>
        val res = dataSourceRepository.versionCommand(path, $(sparkV))
        spark.createDataset[String](res).toDF("value")
      case "add" =>
        val Array(name, version) = path.split("/")
        val url = dataSourceRepository.addCommand(name, version, $(sparkV), $(scalaV))
        val logMsg = format(s"Datasource is loading jar from ${url}")
        logInfo(logMsg)
        dataSourceRepository.loadJarInDriver(url)
        spark.sparkContext.addJar(url)

        //FileUtils.deleteQuietly(new File(url))
        Seq[Seq[String]](Seq(logMsg)).toDF("desc")

    }
  }


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    null
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    null
  }

  final val command: Param[String] = new Param[String](this, "command", "list|version|add", isValid = (m: String) => {
    m == "list" || m == "version" || m == "add"
  })

  final val repository: Param[String] = new Param[String](this, "repository", "repository url")
  final val sparkV: Param[String] = new Param[String](this, "sparkV", "2.3/2.4")
  final val scalaV: Param[String] = new Param[String](this, "scalaV", "2.11/2.12")

  def this() = this(BaseParams.randomUID())
}
