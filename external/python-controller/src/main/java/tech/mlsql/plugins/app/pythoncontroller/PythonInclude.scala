package tech.mlsql.plugins.app.pythoncontroller

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.auth.TableAuthResult
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.includes.analyst.HttpBaseDirIncludeSource
import streaming.dsl.mmlib.algs.param.WowParams
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.version.VersionCompatibility

/**
 * 16/1/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class PythonInclude(override val uid: String) extends SQLAlg with VersionCompatibility with WowParams with ETAuth {
  def this() = this(WowParams.randomUID())


  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val session = df.sparkSession
    import session.implicits._
    val command = JSONTool.parseJson[List[String]](params("parameters")).toArray
    command match {
      case Array(path, "named", tableName) =>
        val format = path.split("\\.").head
        val newPath = path.split("\\.").drop(1).mkString(".")
        val includer = new HttpBaseDirIncludeSource()
        val source = includer.fetchSource(session, newPath, Map("format" -> format))
        val item = JSONTool.toJsonStr(Map("content" -> source))
        val newdf = session.createDataset[String](Seq(item))
        newdf.createOrReplaceTempView(tableName)
        newdf.toDF()
      case _ => throw new RuntimeException("example: !pyInclude python-example.wow.py named wow;")
    }

  }

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    List()
  }

  override def supportedVersions: Seq[String] = {
    Seq("1.5.0-SNAPSHOT", "1.5.0", "1.6.0-SNAPSHOT", "1.6.0")
  }


  override def doc: Doc = Doc(MarkDownDoc,
    s"""
       |
       |```
       |${codeExample.code}
       |```
    """.stripMargin)


  override def codeExample: Code = Code(SQLCode,
    """
      |example
    """.stripMargin)

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

}