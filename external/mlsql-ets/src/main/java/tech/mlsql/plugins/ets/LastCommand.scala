package tech.mlsql.plugins.ets

import java.util.UUID

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.param.WowParams
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.version.VersionCompatibility


class LastCommand(override val uid: String) extends SQLAlg with VersionCompatibility  with WowParams {
  def this() = this(UUID.randomUUID().toString)

  // 
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    val context = ScriptSQLExec.context()
    val command = JSONTool.parseJson[List[String]](params("parameters")).toArray

    //!last named table1;
    context.execListener.getLastSelectTable() match {
      case Some(tableName) =>
        command match {
          case Array("named", newTableName) =>
            val newDf = context.execListener.sparkSession.table(tableName)
            newDf.createOrReplaceTempView(newTableName)
            newDf
        }
      case None => throw new RuntimeException("no table found in previous command")
    }
  }


  override def supportedVersions: Seq[String] = {
    Seq("1.5.0-SNAPSHOT", "1.5.0", "1.6.0-SNAPSHOT", "1.6.0")
  }


  override def doc: Doc = Doc(MarkDownDoc,
    s"""
       |When you want to get the result from command and used
       | in next command(SQL), you can use !last command.
       |
      |For example:
       |
      |```
       |${codeExample.code}
       |```
    """.stripMargin)


  override def codeExample: Code = Code(SQLCode,
    """
      |!hdfs /tmp;
      |!last named hdfsTmpTable;
      |select * from hdfsTmpTable;
    """.stripMargin)

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???
}
