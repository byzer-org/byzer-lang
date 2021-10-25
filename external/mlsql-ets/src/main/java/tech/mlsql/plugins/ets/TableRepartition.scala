package tech.mlsql.plugins.ets

import org.apache.spark.ml.param.{IntParam, Param}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import streaming.dsl.auth.TableAuthResult
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.param.WowParams
import streaming.log.WowLog
import tech.mlsql.common.form.{Dynamic, Extra, FormParams, KV, Select, Text}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.version.VersionCompatibility


class TableRepartition(override val uid: String) extends SQLAlg with VersionCompatibility with Logging with WowLog  with WowParams with ETAuth {
  def this() = this("tech.mlsql.plugins.ets.TableRepartition")

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
        if (params.contains(shuffle.name)){
          logError(format("Error: When partitionType is set to range, shuffle is not supported!"))
        }
        df.repartitionByRange($(partitionNum), $(partitionCols).split(",").map(name => F.col(name)): _*)

      case _ =>
        if (params.contains(shuffle.name) && params.getOrElse(shuffle.name, "true").equals("false")) {
          if (params.contains(partitionCols.name)) {
            logError(format("Error: When shuffle is set to false, partitioning based on the parameter partitionCols is not supported!"))
          }
          return df.coalesce($(partitionNum))
        }
        if (params.contains(partitionCols.name)) {
          df.repartition($(partitionNum))
        } else {
          df.repartition($(partitionNum), $(partitionCols).split(",").map(name => F.col(name)): _*)
        }

    }


  }

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    List()
  }

  override def supportedVersions: Seq[String] = Seq("(1.5.0,)")


  override def doc: Doc = Doc(MarkDownDoc,
    s"""
       |Change the number of partitions. For example, before saving the file, or if we use python, we want python
       |workers to run in parallel as much as possible. At this time, we need use TableRepartition.
    """.stripMargin)


  override def codeExample: Code = Code(SQLCode,
    """
      |Load a JSON data to the `data` table:
      |
      |```sql
      |set jsonStr ='''
      |{"id":0,"parentId":null}
      |{"id":1,"parentId":null}
      |{"id":2,"parentId":1}
      |{"id":3,"parentId":3}
      |{"id":7,"parentId":0}
      |{"id":199,"parentId":1}
      |{"id":200,"parentId":199}
      |{"id":201,"parentId":199}
      |''';
      |
      |load jsonStr.`jsonStr` as data;
      |```
      |
      |Repartition the data table with 2 partitions, as shown below:
      |
      |```
      |run data as TableRepartition.`` where partitionNum="2"
      |as newdata;
      |```
    """.stripMargin)

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  final val partitionNum: IntParam = new IntParam(this, "partitionNum",
    FormParams.toJson(Text(
      name = "partitionNum",
      value = "",
      extra = Extra(
        doc =
          """
            | Required. Number of repartition.
            | e.g. partitionNum = "3"
          """,
        label = "Number of repartition",
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

  final val partitionType: Param[String] = new Param[String](this, "partitionType",
    FormParams.toJson(Select(
      name = "partitionType",
      values = List(),
      extra = Extra(
        doc =
          """
            | Type of repartition. Available values: hash, range. Default: hash.
            | e.g. partitionType = "hash"
          """,
        label = "Type of repartition",
        options = Map(
          "valueType" -> "string",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        List(
          KV(Option("partitionType"), Option("hash")),
          KV(Option("partitionType"), Option("range"))
        )
      })
    )
    ))
  setDefault(partitionType, "hash")

  final val partitionCols: Param[String] = new Param[String](this, "partitionCols",
    FormParams.toJson(Dynamic(
      name = "partitionCols",
      extra = Extra(
        """
          | Column used for repartition, must be specified when partitionType is range.
          | e.g. partitionCols = "col1"
          |""".stripMargin, label = "", options = Map(
          "valueType" -> "string",
          "required" -> "false",
          "derivedType" -> "VALUE_BIND"
        )),
      subTpe = "Select",
      depends = List("partitionType"),
      valueProviderName =
        """
          |set  partitionType="" where type="defaultParam";
          |!if ''' :partitionType == "hash" ''';
          |!then;
          |   select true as enabled, false as required  as result;
          |!else;
          |   select true as enabled, false as required  as result;
          |!fi;
          |select * from result as output;
          |""".stripMargin
    )
    ))

  final val shuffle: Param[String] = new Param[String](this, "shuffle",
    FormParams.toJson(Dynamic(
      name = "shuffle",
      extra = Extra(
        """
          | Whether to start shuffle during the repartition.
          | e.g. shuffle = "true"
          |""".stripMargin, label = "", options = Map(
          "valueType" -> "string",
          "required" -> "false",
          "derivedType" -> "VALUE_BIND"
        )),
      subTpe = "Select",
      depends = List("partitionType"),
      valueProviderName =
        """
          |set  partitionType="" where type="defaultParam";
          |!if ''' :partitionType == "hash" ''';
          |!then;
          |   select true as enabled  as result;
          |!else;
          |   select false as enabled  as result;
          |!fi;
          |select * from result as output;
          |""".stripMargin
    )
    ))
  setDefault(shuffle, "true")

  override def explainParams(sparkSession: SparkSession): DataFrame = _explainParams(sparkSession)

}
