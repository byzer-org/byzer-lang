package tech.mlsql.plugins.ets

import java.sql.Types

import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions => F}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.WowParams

/**
 * 27/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class SchemaCommand(override val uid: String) extends SQLAlg with WowParams {
  def this() = this(Identifiable.randomUID("tech.mlsql.plugins.ets.SchemaCommand"))

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val session = df.sparkSession
    params("operate") match {
      case "show tables" => {
        val catalog = params("catalog")
        session.sql(s"show tables from ${catalog}").
          select(
            F.lit(catalog).as("TABLE_CAT"),
            F.lit("").as("TABLE_SCHEM"),
            F.col("tableName").as("TABLE_NAME"),
            F.lit("").as("TABLE_TYPE")
          )
      }
      case "show databases" => {
        session.sql("show databases").select(F.col("databaseName").as("TABLE_CAT"))
      }
      case "tableSchema" =>
        val fields = df.sparkSession.table(params("tableName")).schema.fields.map { item =>
          Row.fromSeq(Seq(item.name, item.dataType.typeName, SchemaCommand.sqlType(item.dataType.typeName), item.nullable))
        }
        val rows = df.sparkSession.sparkContext.parallelize(fields)
        df.sparkSession.createDataFrame(rows, StructType(Seq(
          StructField("name", StringType, true),
          StructField("dataType", StringType, true),
          StructField("sqlType", IntegerType, true),
          StructField("nullable", BooleanType, true)))).toDF
      case "getColumns" =>

        val fields = df.sparkSession.table(params("tableName")).schema.fields.map { item =>
          Row.fromSeq(Seq(
            params("catalog"),
            "",
            params("tableName"),
            item.name,
            SchemaCommand.sqlType2(item.dataType.typeName),
            "UNKNOWN",
            0,
            65535,
            0,
            0,
            0,
            "",
            "",
            0,
            0,
            0,
            0,
            "",
            "NO",
            "",
            "",
            1,
            "YES",
            ""

          ))
        }
        val rows = df.sparkSession.sparkContext.parallelize(fields)
        df.sparkSession.createDataFrame(rows, StructType(Seq(
          StructField("TABLE_CAT", StringType, true),
          StructField("TABLE_SCHEM", StringType, true),
          StructField("TABLE_NAME", StringType, true),
          StructField("COLUMN_NAME", StringType, true),
          StructField("DATA_TYPE", IntegerType, true),
          StructField("TYPE_NAME", StringType, true),
          StructField("COLUMN_SIZE", IntegerType, true),
          StructField("BUFFER_LENGTH", IntegerType, true),
          StructField("DECIMAL_DIGITS", IntegerType, true),
          StructField("NUM_PREC_RADIX", IntegerType, true),
          StructField("NULLABLE", IntegerType, true),
          StructField("REMARKS", StringType, true),
          StructField("COLUMN_DEF", StringType, true),
          StructField("SQL_DATA_TYPE", IntegerType, true),
          StructField("SQL_DATETIME_SUB", IntegerType, true),
          StructField("CHAR_OCTET_LENGTH", IntegerType, true),
          StructField("ORDINAL_POSITION", IntegerType, true),
          StructField("IS_NULLABLE", StringType, true),
          StructField("SCOPE_CATALOG", StringType, true),
          StructField("SCOPE_SCHEMA", StringType, true),
          StructField("SCOPE_TABLE", StringType, true),
          StructField("SOURCE_DATA_TYPE", IntegerType, true),
          StructField("IS_AUTOINCREMENT", StringType, true),
          StructField("IS_GENERATEDCOLUMN", StringType, true)
        )
        )).toDF
    }


  }


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???
}

object SchemaCommand {
  def sqlType(t: String) = {
    t match {
      case "integer" => Types.INTEGER
      case "double" => Types.DOUBLE
      case "long" => Types.BIGINT
      case "float" => Types.FLOAT
      case "short" => Types.SMALLINT
      case "byte" => Types.TINYINT
      case "boolean" => Types.BIT
      case "string" => Types.CLOB
      case "binary" => Types.BLOB
      case "timestamp" => Types.TIMESTAMP
      case "date" => Types.DATE
      case "decimal" => Types.DECIMAL
    }
  }

  def sqlType2(t: String) = {
    t match {
      case "integer" => Types.INTEGER
      case "double" => Types.DOUBLE
      case "long" => Types.BIGINT
      case "float" => Types.FLOAT
      case "short" => Types.SMALLINT
      case "byte" => Types.TINYINT
      case "boolean" => Types.BIT
      case "string" => Types.CHAR
      case "binary" => Types.BLOB
      case "timestamp" => Types.TIMESTAMP
      case "date" => Types.DATE
      case "decimal" => Types.DECIMAL
    }
  }
}
