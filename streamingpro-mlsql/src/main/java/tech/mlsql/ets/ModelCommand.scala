package tech.mlsql.ets

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.BaseParams
import streaming.dsl.mmlib.algs.{Functions, MllibFunctions}
import tech.mlsql.common.utils.hdfs.HDFSOperator
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.ets.alg.BaseAlg


/**
  * 2019-05-10 WilliamZhu(allwefantasy@gmail.com)
  */
class ModelCommand(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseAlg with BaseParams {
  def this() = this(BaseParams.randomUID())

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession

    if (!isModelPath(path)) throw new MLSQLException(s"$path is not a validate model path")

    val paths = HDFSOperator.listFiles(path).map(file => PathFun(path).add(file.getPath.getName).toPath)

    var modelPaths = paths.filter(f => f.split("/").last.startsWith("_model_"))
    val keepVersion = modelPaths.size > 0


    def getModelMetaData(spark: SparkSession, path: String): Option[DataFrame] = {
      val metaDataPath = PathFun(path).add("meta").add("0").toPath
      if (HDFSOperator.fileExists(metaDataPath) && HDFSOperator.listFiles(metaDataPath).
        filter(f => f.getPath.getName.endsWith(".parquet")).size > 0) {
        return Option(spark.read.parquet(metaDataPath))
      }
      return None
    }

    val context = ScriptSQLExec.contextGetOrForTest()
    params.get(action.name) match {
      case Some("history") =>
        modelPaths = if (keepVersion) modelPaths else Seq(path)
        val model = modelPaths.sortBy(metaPath => metaPath).reverse.map { metaPath =>
          (path.substring(context.home.length), getModelMetaData(spark, metaPath))
        }.filter(f => f._2.isDefined).map(f => formatOutputWithMultiColumns(f._1, f._2.get)).reduce(_ union _)
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