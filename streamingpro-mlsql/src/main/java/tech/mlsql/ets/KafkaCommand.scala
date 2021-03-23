package tech.mlsql.ets


import org.apache.spark.sql.execution.datasources.json.WowJsonInferSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.kafka010.MLSQLKafkaOffsetInfo
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.unsafe.types.UTF8String
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth._
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.MLSQLEnvKey
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.tool.HDFSOperatorV2

/**
 * 2019-06-03 WilliamZhu(allwefantasy@gmail.com)
 */
class KafkaCommand(override val uid: String) extends SQLAlg with ETAuth with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  /*
  !kafkaTool sampleData 20 records from "127.0.0.1:9092" wow;
   */
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    if (path != "kafka") {
      throw new MLSQLException("KafkaCommand only support Kafka broker version 0.10.0 or higher")
    }

    val command = JSONTool.parseJson[List[String]](params("parameters"))


    val spark = df.sparkSession
    import spark.implicits._


    // !kafkaTool streamOffset ck
    command match {
      case List("streamOffset", ckPath, _*) =>
        val context = ScriptSQLExec.contextGetOrForTest()
        val offsetPath = PathFun(context.home).add(ckPath).add("offsets").toPath
        val lastFile = HDFSOperatorV2.listFiles(offsetPath)
          .filterNot(_.getPath.getName.endsWith(".tmp.crc"))
          .map { fileName =>
            (fileName.getPath.getName.split("/").last.toInt, fileName.getPath)
          }.maxBy(f => f._1)._2

        val content = HDFSOperatorV2.readFile(lastFile.toString)
        val offsets = content.split("\n").last
        val desc =
          """
            |-- the stream name, should be uniq.
            |set streamName="kafkaStreamExample";
            |
            |!kafkaTool registerSchema 2 records from "127.0.0.1:9092" wow;
            |
            |load kafka.`wow` options
            |startingOffsets='''
            | PUT THE OFFSET JSON HERE
            |'''
            |and kafka.bootstrap.servers="127.0.0.1:9092"
            |as newkafkatable1;
          """.stripMargin
        return spark.createDataset[(String, String)](Seq((offsets, desc))).toDF("offsets", "doc")
      case _ =>
    }

    //action: sampleData,schemaInfer
    val action = command.head

    val parameters = Map("sampleNum" -> command(1), "subscribe" -> command(5), "kafka.bootstrap.servers" -> command(4))
    val (startOffset, endOffset) = MLSQLKafkaOffsetInfo.getKafkaInfo(spark, parameters)

    var reader = spark
      .read
      .format("kafka")
      .options(parameters)
    if (startOffset != null) {
      reader = reader.option("startingOffsets", startOffset.json)
    }
    if (endOffset != null) {
      reader = reader.option("endingOffsets", endOffset.json)
    }

    val newDF = reader.option("failOnDataLoss", "false").load()
    val res = newDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val context = ScriptSQLExec.contextGetOrForTest()
    val sampleNum = params.getOrElse("sampleNum", "100").toLong

    def inferSchema = {
      val data = res.collect().map(f => UTF8String.fromString(f._2)).toSeq
      val schema = WowJsonInferSchema.inferJson(data, spark)
      val schemaStr = serializeSchema(schema)
      logInfo(format(s"Infer schema: ${schemaStr}"))
      schemaStr
    }

    action match {
      case "sampleData" => res.limit(sampleNum.toInt).toDF()
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
    DataType.fromJson(json) match {
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

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val command = JSONTool.parseJson[List[String]](params("parameters"))
    val action = command.head
    if (Set("streamOffset", "help", "").contains(action)) return List()
    val parameters = Map("sampleNum" -> command(1), "subscribe" -> command(5), "kafka.bootstrap.servers" -> command(4))
    val vtable = MLSQLTable(
      None,
      Option(parameters("subscribe")),
      OperateType.LOAD,
      Option("kafka"),
      TableType.KAFKA)

    val context = ScriptSQLExec.contextGetOrForTest()
    context.execListener.getTableAuth match {
      case Some(tableAuth) =>
        tableAuth.auth(List(vtable))
      case None => List(TableAuthResult(true, ""))
    }
  }
}
