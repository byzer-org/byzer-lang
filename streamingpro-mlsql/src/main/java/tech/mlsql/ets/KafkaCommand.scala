package tech.mlsql.ets


import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
import org.apache.spark.sql.execution.datasources.json.WowJsonInferSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.kafka010.MLSQLKafkaOffsetInfo
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.unsafe.types.UTF8String
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.MLSQLEnvKey

import scala.util.Try

/**
  * 2019-06-03 WilliamZhu(allwefantasy@gmail.com)
  */
class KafkaCommand(override val uid: String) extends SQLAlg with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    if (path != "kafka") {
      throw new MLSQLException("KafkaCommand only support Kafka broker version 0.10.0 or higher")
    }
    val spark = df.sparkSession
    import spark.implicits._
    val parameters = params - "action"
    val (startOffset, endOffset) = MLSQLKafkaOffsetInfo.getKafkaInfo(spark, parameters)

    //action: sampleData,schemaInfer
    val action = params.getOrElse("action", "sampleData")

    val newdf = spark
      .read
      .format("kafka")
      .options(parameters)
      .option("startingOffsets", startOffset.json)
      .option("endingOffsets", endOffset.json)
      .load()
    val res = newdf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val context = ScriptSQLExec.contextGetOrForTest()

    def inferSchema = {
      val data = res.collect().map(f => UTF8String.fromString(f._2)).toSeq
      val schema = WowJsonInferSchema.inferJson(data, spark)
      val schemaStr = serializeSchema(schema)
      logInfo(format(s"Infer schema: ${schemaStr}"))
      schemaStr
    }

    action match {
      case "sampleData" => res.toDF()
      case "schemaInfer" =>
        Seq(Seq(inferSchema)).toDF("value")
      case "registerSchema" =>
        val schemaStr = inferSchema
        context.execListener.addEnv(MLSQLEnvKey.CONTEXT_KAFKA_SCHEMA, schemaStr)
        Seq(Seq(schemaStr)).toDF("value")
    }

  }


  def serializeSchema(schema: StructType): String = schema.json

  def deserializeSchema(json: String): StructType = {
    Try(DataType.fromJson(json)).getOrElse(LegacyTypeStringParser.parse(json)) match {
      case t: StructType => t
      case _ => throw new RuntimeException(s"Failed parsing StructType: $json")
    }
  }


  override def skipPathPrefix: Boolean = true

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new MLSQLException(s"${getClass.getName} not support register ")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new MLSQLException(s"${getClass.getName} not support register ")
  }
}
