package tech.mlsql.ets

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.dsl.auth.BaseETAuth
import tech.mlsql.version.VersionCompatibility

/**
 * 26/10/2021 WilliamZhu(allwefantasy@gmail.com)
 */
abstract class BaseScriptAlgExt(override val uid: String) extends SQLAlg with VersionCompatibility with Functions with WowParams with BaseETAuth {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val inputTable = params("__dfname__")
    val outputTable = params("__newdfname__")

    val newParams = params - "__dfname__" - "__LINE__" - "__newdfname__"

    val scriptParams = List(s"set inputTable='''${inputTable}''';", s"set outputTable='''${outputTable}''';") ++ newParams.map { case (k, v) =>
      s"""set ${k}='''${v}''';"""
    }
    val scriptStr = scriptParams.mkString("\n") + "\n" + code_template
    val newDF: DataFrame = ScriptRunner.rubSubJob(
      scriptStr,
      (_df: DataFrame) => {},
      Option(df.sparkSession),
      true,
      true).get
    newDF
  }

  def code_template: String


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def supportedVersions: Seq[String] = Seq(">=2.1.0")

  override def skipPathPrefix: Boolean = true

  override def skipOriginalDFName: Boolean = false

  override def skipResultDFName: Boolean = false
}
