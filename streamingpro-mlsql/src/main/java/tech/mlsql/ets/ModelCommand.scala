package tech.mlsql.ets

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import streaming.common.{HDFSOperator, PathFun}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.BaseParams
import streaming.dsl.mmlib.algs.{Functions, MllibFunctions}


/**
  * 2019-05-10 WilliamZhu(allwefantasy@gmail.com)
  */
class ModelCommand(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseParams {
  def this() = this(BaseParams.randomUID())

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession

    val paths = HDFSOperator.listFiles(path).map(file => PathFun(path).add(file.getPath.getName).toPath)

    if (paths.isEmpty) return emptyDataFrame(spark, "value")

    val keepVersion = paths.head.split("/").last.startsWith("_model_")


    def getModelMetaData(spark: SparkSession, path: String): DataFrame = {
      if (keepVersion) {
        val metaDataPath = PathFun(path).add("meta").add("0").toPath
        if (HDFSOperator.fileExists(metaDataPath)) {
          return spark.read.parquet(metaDataPath)
        }
      }
      return emptyDataFrame(spark, "value")
    }

    val context = ScriptSQLExec.contextGetOrForTest()
    params.get(action.name) match {
      case Some("history") =>
        val model = paths.sortBy(metaPath => metaPath).reverse.map { metaPath =>
          formatOutputWithMultiColumns(path.substring(context.home.length), getModelMetaData(spark, metaPath))
        }.reduce(_ union _)
        model.orderBy(F.desc("modelPath"))
      case _ => throw new MLSQLException("no command found")
    }


  }


  override def skipPathPrefix: Boolean = false

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new MLSQLException(s"${getClass.getName} not support register ")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new MLSQLException(s"${getClass.getName} not support register ")
  }

  final val action: Param[String] = new Param[String](this, "action", "")

}