package tech.mlsql.ets

import org.apache.spark.ml.param.{IntParam, Param}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.form.{Extra, FormParams, Text}
import tech.mlsql.schema.parser.SparkSimpleSchemaParser

/**
 * 22/11/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class EmptyTableWithSchema(override val uid: String) extends SQLAlg with WowParams {
  def this() = this(BaseParams.randomUID())


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val session = df.sparkSession
    val schema = schemaFromStr(params("schema"))
    session.createDataFrame(session.sparkContext.emptyRDD[Row]
      , schema)
  }

  private def schemaFromStr(schemaStr: String) = {
    val targetSchema = schemaStr.trim match {
      case item if item.startsWith("{") =>
        DataType.fromJson(schemaStr).asInstanceOf[StructType]
      case item if item.startsWith("st") =>
        SparkSimpleSchemaParser.parse(schemaStr).asInstanceOf[StructType]
      case item if item == "file" =>
        SparkSimpleSchemaParser.parse("st(field(start,long),field(offset,long),field(value,binary))").asInstanceOf[StructType]
      case _ =>
        StructType.fromDDL(schemaStr)
    }
    targetSchema
  }

  final val schema: Param[String] = new Param[String](this, "schema",
    FormParams.toJson(Text(
      name = "schema",
      value = "",
      extra = Extra(
        doc =
          """
            | schema
          """,
        label = "schema",
        options = Map(
          "valueType" -> "string",
          "defaultValue" -> "",
          "required" -> "true",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )
  setDefault(schema, "")


  override def skipPathPrefix: Boolean = true

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new MLSQLException(s"${getClass.getName} not support register ")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new MLSQLException(s"${getClass.getName} not support register ")
  }

}
