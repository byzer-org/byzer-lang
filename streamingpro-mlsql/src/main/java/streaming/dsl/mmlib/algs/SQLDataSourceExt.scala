package streaming.dsl.mmlib.algs

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.core.datasource.DataSourceRepository
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

/**
  * 2019-01-14 WilliamZhu(allwefantasy@gmail.com)
  */
class SQLDataSourceExt(override val uid: String) extends SQLAlg with WowParams {


  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val repository = new DataSourceRepository()
    params.get(command.name).map { item =>
      set(command, item)
      item
    }.getOrElse {
      set(command, "list")
    }
    val spark = df.sparkSession
    import spark.implicits._
    $(command) match {
      case "list" =>
        spark.read.json(spark.createDataset(repository.listCommand))
      case "version" =>

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

  def this() = this(BaseParams.randomUID())
}
