package streaming.dsl.mmlib

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * Created by allwefantasy on 13/1/2018.
  */
trait SQLAlg extends Serializable {
  def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame

  def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any

  def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction

  def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val sparkSession = df.sparkSession
    import sparkSession.implicits._
    Seq.empty[(String, String)].toDF("param", "description")
  }

  def explainParams(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    Seq.empty[(String, String)].toDF("param", "description")
  }

  def explainModel(sparkSession: SparkSession, path: String, params: Map[String, String]): DataFrame = {
    import sparkSession.implicits._
    Seq.empty[(String, String)].toDF("key", "value")
  }

  def skipPathPrefix: Boolean = false

  def modelType: ModelType = UndefinedType

  def doc: Doc = Doc(TextDoc, "")

  def codeExample: Code = Code(SQLCode, "")

}

sealed abstract class ModelType
(
  val name: String,
  val humanFriendlyName: String
)

case object AlgType extends ModelType("algType", "algorithm")

case object ProcessType extends ModelType("processType", "feature engineer")

case object UndefinedType extends ModelType("undefinedType", "undefined")


case class Doc(docType: DocType, doc: String)

sealed abstract class DocType
(
  val docType: String
)

case object HtmlDoc extends DocType("html")

case object TextDoc extends DocType("text")


case class Code(codeType: CodeType, code: String)

sealed abstract class CodeType
(
  val codeType: String
)

case object SQLCode extends CodeType("sql")

case object ScalaCode extends CodeType("scala")

case object PythonCode extends CodeType("python")



