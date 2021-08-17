package tech.mlsql.plugins.ets

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, StringType, StructField, StructType}
import streaming.dsl.{MLSQLExecuteContext, ScriptSQLExec, ScriptSQLExecListener}
import streaming.dsl.mmlib.SQLAlg

object JsonExpandExtSuite {
  val oneKVJson: Seq[String] = Seq(""" {"key":"value"} """)
  val oneKVSchema = StructType(Seq(
    StructField("key", StringType)
  ))
  val complexJsonSeq1 = Seq(
    """{
      | "name":  "n1",
      | "price": 0.1,
      | "boolean": true,
      | "tags": ["tag_1", "tag_2", "tag_3"],
      | "nested": {"key_1":"value_1", "key_2": "value_2"},
      | "emptyString": ""
      |}""".stripMargin,
    """{
      | "name":  "n2",
      | "price": 0.1,
      | "boolean": false,
      | "city": "北京"
      |}""".stripMargin
  )
  val complexSchema1: StructType = StructType(Seq(
    StructField("boolean", BooleanType)
    , StructField("city", StringType)
    , StructField("emptyString", StringType)
    , StructField("name", StringType)
    , StructField("nested", StructType(Seq(StructField("key_1", StringType), StructField("key_2", StringType))))
    , StructField("price", DoubleType)
    , StructField("tags", ArrayType(StringType, true))
  ))
  val invalidJsonSeq1 = Seq(
    """{"key","value"}""".stripMargin
  )
  val param = Map("inputCol" -> "col1")
  lazy val sparkSession: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName(JsonExpandExtSuite.clazz)
    .getOrCreate()

  val clazz = classOf[JsonExpandExt].getName
  lazy val alg = Class.forName(clazz).newInstance().asInstanceOf[SQLAlg]

}

import JsonExpandExtSuite._

class JsonExpandExtSuite extends ETSUnitTestSuite {

  override def beforeAll(): Unit = {
    // set SparkContext in ScriptSQLExec
    val listener = new ScriptSQLExecListener(sparkSession, "/", Map("test" -> "test"))
    val mlsqlCtx =  MLSQLExecuteContext( listener,
      "zjc",
      "/tmp",
      "111",
      param
    )
    ScriptSQLExec.setContext(mlsqlCtx)
  }

  override def afterAll(): Unit = {
    sparkSession.stop
  }

  test("JsonExpandExt should expand valid json string") {
    import sparkSession.implicits._

    val inDf1 = sparkSession.sparkContext.parallelize(oneKVJson, 1).toDF("col1")
    val outDf1 = alg.train(inDf1, "", param)
    assert( outDf1.schema == oneKVSchema )
  }

  test("JsonExpandExt should respect samplingRatio") {
    import sparkSession.implicits._
    val inDf2 = sparkSession.sparkContext.parallelize(complexJsonSeq1, 1).toDF("col1")
    val outDf2 = alg.train(inDf2, "", param + ("samplingRatio" -> "1.0"))
    assert( outDf2.schema == complexSchema1)
  }

  test("JsonExpandExt should throw exception in case of invalid json string") {
    import sparkSession.implicits._

    val inDf3 = sparkSession.sparkContext.parallelize(invalidJsonSeq1, 1).toDF("col1")
    val thrown = intercept[RuntimeException] {
      alg.train(inDf3 , "", param)
    }
    assert(thrown.getMessage.startsWith("Corrupted JSON in column"))
  }

  test("JsonExpandExt should throw AnalysisException if column does not exist") {
    import sparkSession.implicits._

    val inDf3 = sparkSession.sparkContext.parallelize(invalidJsonSeq1, 1).toDF("col1")
    val thrown = intercept[AnalysisException] {
      alg.train(inDf3 , "", Map("inputCol" -> "col_2"))
    }
    assert(thrown.getMessage.startsWith("cannot resolve "))
  }

}
