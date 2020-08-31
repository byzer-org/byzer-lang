package tech.mlsql.plugins.ets

import java.io.File

import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.TableAuthResult
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.WowParams
import tech.mlsql.common.utils.hdfs.HDFSOperator
import tech.mlsql.dsl.adaptor.{DslAdaptor, DslTool}
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod

/**
 * 31/8/2020 WilliamZhu(allwefantasy@gmail.com)
 * !saveFile _ -i tableName -o /tmp/wow.xlsx;
 */
class SaveBinaryAsFile(override val uid: String) extends SQLAlg with DslTool with ETAuth with WowParams {
  def this() = this(Identifiable.randomUID("tech.mlsql.plugins.ets.SaveBinaryAsFile"))

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val _filePath = params(filePath.name)
    val finalPath = resourceRealPath(context.execListener, Option(context.owner), _filePath)
    val bytes = df.collect().head.getAs[Array[Byte]](0)
    val file = new File(finalPath)
    HDFSOperator.saveBytesFile(file.getParentFile.getPath, file.getName, bytes)
    df.sparkSession.emptyDataFrame

  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    List()
  }

  final val filePath: Param[String] = new Param[String](this, "filePath",
    """
      |file name
      |""".stripMargin)

}
