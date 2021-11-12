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
        if (!params.contains(partitionCols.name)) {
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
      |### Example 1: Use of partitionNum
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
      |
      |### Example 2: Use of partitionType
      |
      |partitionType supports the following two configurations:
      |
      |  -hash: repartition uses `HashPartitioner`, the purpose is to evenly distribute data on the number of partitions provided. If one column (or more columns) is provided, these values ​​will be hashed and the partition number will be determined by calculating `partition = hash(columns)% numberOfPartitions`.
      |
      |  -range: The repartition application `RangePartitioner` will partition the data according to the range of column values. This is usually used for continuous (non-discrete) values, such as any type of number.
      |
      |Here are some demos to demonstrate this difference.
      |
      |
      |**Test Dataframes**
      |
      |The following JSON data is used in this demo:
      |
      |```
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
      |
      |```
      |
      |
      |
      |All test results use the following retrieve data logic (the SQL should be placed after the TableRepartition statement for retrieve data):
      |
      |```
      |!profiler sql'''
      |select spark_partition_id() as partition,min(id) as min_id,max(id) as max_id,count(id) as count
      |from simpleData
      |group by partition
      |order by partition;''';
      |```
      |
      |The partitionType="hash" code is as follows:
      |```
      |run data as TableRepartition.`` where partitionNum="3" and partitionType="hash" as simpleData;
      |```
      |
      |As expected, we got 3 partitions, and the ids were hashed to different partitions. The results are as follows:
      |
      || partition | min_id | max_id | count |
      || :-------- | :----- | :----- | :---- |
      || 0 | 2 | 7 | 3 |
      || 1 | 0 | 0 | 1 |
      || 2 | 1 | 201 | 4 |
      |
      |The partitionType="range" code is as follows:
      |
      |```
      |run data as TableRepartition.`` where partitionNum="3" and partitionType="range" and partitionCols="id" as simpleData;
      |```
      |
      |Also in this example, we got 3 partitions, but this time the minimum and maximum values clearly show the range of values in the partitions. The results are as follows:
      |
      || partition | min_id | max_id | count |
      || :-------- | :----- | :----- | :---- |
      || 0 | 0 | 2 | 3 |
      || 1 | 3 | 199 | 3 |
      || 2 | 200 | 201 | 2 |
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
          "valueType" -> "int",
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
          "derivedType" -> "DYNAMIC_BIND"
        )),
      subTpe = "Text",
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
          "derivedType" -> "DYNAMIC_BIND"
        )),
      subTpe = "Select",
      depends = List("partitionType"),
      valueProviderName =
        """
          |set partitionType="" where type="defaultParam";
          |!if ''' :partitionType == "hash" ''';
          |!then;
          |   select true as enabled,'''[{"name":"suffle","value":"true"},{"name":"suffle","value":"false"}]''' as values as result;
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