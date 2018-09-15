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

  def explainParams(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    Seq.empty[(String, String)].toDF("param", "description")
  }

  def skipPathPrefix: Boolean = false

  def modelType: ModelType = UndefinedType

  def doc: String = ""

  def codeExample: String = ""

}

sealed abstract class ModelType
(
  val name: String,
  val humanFriendlyName: String
)

case object AlgType extends ModelType("algType", "algorithm")

case object ProcessType extends ModelType("processType", "feature engineer")

case object UndefinedType extends ModelType("undefinedType", "undefined")

