package tech.mlsql.plugins.ets

import org.apache.spark.ml.param.{IntParam, Param}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import streaming.dsl.auth.TableAuthResult
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.param.WowParams
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.version.VersionCompatibility


class TableRepartition(override val uid: String) extends SQLAlg with VersionCompatibility  with WowParams with ETAuth {
  def this() = this("tech.mlsql.plugins.ets.TableRepartition")

  // 
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    params.get(partitionNum.name).map { item =>
      set(partitionNum, item.toInt)
      item
    }.getOrElse {
      throw new MLSQLException(s"${partitionNum.name} is required")
    }

    params.get(partitionType.name).map { item =>
      set(partitionType, item)
      item
    }.getOrElse {
      set(partitionType, "hash")
    }

    params.get(partitionCols.name).map { item =>
      set(partitionCols, item)
      item
    }.getOrElse {
      set(partitionCols, "")
    }

    $(partitionType) match {
      case "range" =>

        require(params.contains(partitionCols.name), "At least one partition-by expression must be specified.")
        df.repartitionByRange($(partitionNum), $(partitionCols).split(",").map(name => F.col(name)): _*)

      case _ =>
        df.repartition($(partitionNum))
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
    """.stripMargin)


  override def codeExample: Code = Code(SQLCode,
    """
      |
    """.stripMargin)

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  final val partitionNum: IntParam = new IntParam(this, "partitionNum",
    "")
  final val partitionType: Param[String] = new Param[String](this, "partitionType",
    "")

  final val partitionCols: Param[String] = new Param[String](this, "partitionCols",
    "")

  override def explainParams(sparkSession: SparkSession): DataFrame = _explainParams(sparkSession)

}
