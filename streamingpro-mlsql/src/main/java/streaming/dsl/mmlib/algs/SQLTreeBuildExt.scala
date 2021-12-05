/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streaming.dsl.mmlib.algs

import net.liftweb.json.NoTypeHints
import net.liftweb.{json => SJSon}
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions => F}
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.form.{Extra, FormParams, KV, Select, Text}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 2018-12-12 WilliamZhu(allwefantasy@gmail.com)
  */
class SQLTreeBuildExt(override val uid: String) extends SQLAlg with Functions with WowParams {

  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    params.get(idCol.name).
      map(m => set(idCol, m)).getOrElse {
      throw new MLSQLException("idCol is required")
    }

    params.get(parentIdCol.name).
      map(m => set(parentIdCol, m)).getOrElse {
      throw new MLSQLException("parentIdCol is required")
    }

    params.get(topLevelMark.name).
      map(m => set(topLevelMark, m)).getOrElse {
      set(topLevelMark, null)
    }

    params.get(treeType.name).
      map(m => set(treeType, m)).getOrElse {
      set(treeType, "treePerRow")
    }

    params.get(recurringDependencyBreakTimes.name).
      map(m => set(recurringDependencyBreakTimes, m.toInt)).getOrElse {
      set(recurringDependencyBreakTimes, 1000)
    }

    val maxTimes = $(recurringDependencyBreakTimes)

    val parentIdColType = df.schema.filter(f => f.name == $(parentIdCol)).head
    val t = if ($(topLevelMark) != null) {
      parentIdColType.dataType match {
        case s: IntegerType => $(topLevelMark).toInt
        case s: LongType => $(topLevelMark).toLong
        case s: DoubleType => $(topLevelMark).toDouble
        case s: ShortType => $(topLevelMark).toShort
        case _ => $(topLevelMark)
      }
    } else {
      null
    }
    val items = df.select($(idCol), $(parentIdCol)).distinct().rdd.filter(row => row.get(0) != row.get(1)).map { row => IDParentID(row.get(0), row.get(1), ArrayBuffer()) }.collect()
    val ROOTS = ArrayBuffer[IDParentID]()
    val tempMap = scala.collection.mutable.HashMap[Any, Int]()
    val itemsWithIndex = items.zipWithIndex
    itemsWithIndex.foreach { case (item, index) =>
      tempMap(item.id) = index
    }
    itemsWithIndex.foreach { case (item, index) =>

      if (item.parentID != null || item.parentID != t) {
        items(tempMap(item.parentID)).children += item
      } else {
        ROOTS += item
      }
    }
    implicit val formats = SJSon.Serialization.formats(NoTypeHints)
    val rdd = df.sparkSession.sparkContext.parallelize(ROOTS.map(f => SJSon.Serialization.write(f)))
    var newdf = df.sparkSession.read.json(rdd)


    val computeLevel1 = (a: Seq[Row], level: Int) => {
      val computeLevel = new ((Seq[Row], Int) => Int) {
        def apply(a: Seq[Row], level: Int): Int = {
          if (a.size == 0) return level
          if (level > maxTimes) return level
          return a.map { row =>
            val index = a.head.schema.zipWithIndex.filter(s => s._1.name == "children").head._2
            val value = row.getSeq[Row](index)
            apply(value, level + 1)
          }.max

        }
      }
      computeLevel(a, level)
    }
    val computeLevelUDF = F.udf(computeLevel1)
    newdf = newdf.withColumn("level", computeLevelUDF(F.col("children"), F.lit(0)))

    $(treeType) match {
      case "treePerRow" => newdf
      case "nodeTreePerRow" =>

        val rdd = df.sparkSession.sparkContext.parallelize(items.toSeq).map { item =>

          val resultset = new mutable.HashSet[IDParentID]()
          var level = 0
          val collectAll = new ((IDParentID) => Int) {
            def apply(a: IDParentID): Int = {
              if (a.children.size == 0) {
                resultset += a
                1
              } else {
                resultset ++= a.children
                a.children.map(f => apply(f)).sum
              }
            }
          }
          val computeLevel = new ((IDParentID, Int) => Int) {
            def apply(a: IDParentID, level: Int): Int = {
              if (a.children.size == 0) return level
              if (level > maxTimes) return level
              return a.children.map { row =>
                apply(row, level + 1)
              }.max
            }
          }
          level = computeLevel(item, 0)
          if (level < maxTimes) {
            if (item.children.size > 0) {
              collectAll(item)
            }
          }

          Row.fromSeq(Seq(item.id.toString, level, resultset.map(f => f.id.toString).toSeq))
        }
        df.sparkSession.createDataFrame(rdd, StructType(Seq(
          StructField(name = "id", dataType = StringType), StructField(name = "level", dataType = IntegerType), StructField(name = "children", dataType = ArrayType(StringType))
        )))

    }


  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new RuntimeException("register is not support by this estimator/transformer")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new RuntimeException("register is not support by this estimator/transformer")
  }


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  override def explainParams(sparkSession: SparkSession): DataFrame = _explainParams(sparkSession)

  override def modelType: ModelType = ProcessType

  override def doc: Doc = Doc(HtmlDoc,
    """
      |  TreeBuildExt used to build a tree when you have father - child relationship in some table,
      |  please check the codeExample to see how to use it.
    """.stripMargin)


  override def codeExample: Code = Code(SQLCode, CodeExampleText.jsonStr +
    """
      |```sql
      |set jsonStr = '''
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
      |set spark.sql.legacy.allowUntypedScalaUDF=true where type="conf";
      |run data as TreeBuildExt.`` where idCol="id" and parentIdCol="parentId" and treeType="nodeTreePerRow" as result;
      |```
      |If you are currently using spark3 version, you need to set the following parameters:
      |
      |```
      |set spark.sql.legacy.allowUntypedScalaUDF=true where type="conf";
      |```
      |
      |Here are the result:
      |
      |```
      |+---+-----+------------------+
      ||id |level|children          |
      |+---+-----+------------------+
      ||200|0    |[]                |
      ||0  |1    |[7]               |
      ||1  |2    |[200, 2, 201, 199]|
      ||7  |0    |[]                |
      ||201|0    |[]                |
      ||199|1    |[200, 201]        |
      ||2  |0    |[]                |
      |+---+-----+------------------+
      |```
      |
      |Notice that we will convert the id to string in final result. That means id is string type, and children are array of
      |string and you should be careful when comparing.
      |
      |The max level should lower than 1000(You can set by parameter recurringDependencyBreakTimes).
      |When you found some rows are weired, the level >= 1000 and the children is empty, this means
      |there are recurring dependency and we can not deal with this situation yet.
      |
      |Here is the example:
      |
      |```
      |+---+-----+------------------+
      ||id |level|children          |
      |+---+-----+------------------+
      ||7  |1000    |[]             |
      |```
      |
      |if treeType == treePerRow
      |
      |then the result is :
      |
      |+----------------------------------------+---+--------+-----+
      ||children                                |id |parentID|level|
      |+----------------------------------------+---+--------+-----+
      ||[[[], 7, 0]]                            |0  |null    |1    |
      ||[[[[[], 200, 199]], 199, 1], [[], 2, 1]]|1  |null    |2    |
      |+----------------------------------------+---+--------+-----+
      |
      |Notice that children's datatype is Row, you can change it to json so you can use python to deal with it.
      |
    """.stripMargin)

  override def coreCompatibility: Seq[CoreVersion] = super.coreCompatibility

  final val idCol: Param[String] = new Param[String](this, "idCol",
    FormParams.toJson(Text(
      name = "idCol",
      value = "",
      extra = Extra(
        doc =
          """
            | Required. Id used column
          """,
        label = "Id used column",
        options = Map(
          "valueType" -> "string",
          "required" -> "true",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      }))))

  final val parentIdCol: Param[String] = new Param[String](this, "parentIdCol",
    FormParams.toJson(Text(
      name = "parentIdCol",
      value = "",
      extra = Extra(
        doc =
          """
            | Required. Parent id used column
          """,
        label = "Parent id used column",
        options = Map(
          "valueType" -> "string",
          "required" -> "true",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      }))))

  final val topLevelMark: Param[String] = new Param[String](this, "topLevelMark",
    FormParams.toJson(Text(
      name = "topLevelMark",
      value = "",
      extra = Extra(
        doc =
          """
            | The mark of top level
          """,
        label = "The mark of top level",
        options = Map(
          "valueType" -> "string",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )

  final val treeType: Param[String] = new Param[String](this, "treeType",
    FormParams.toJson(Select(
      name = "treeType",
      values = List(),
      extra = Extra(
        doc =
          """
            | The type of parent-child relationship SQL tree.
            | - treePerRow: Need to display the hierarchical relationship as a nested row structure
            | - nodeTreePerRow: Each layer is displayed as a line
          """,
        label = "action for syntax analysis",
        options = Map(
          "valueType" -> "string",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        List(KV(Option("treeType"), Option("treePerRow")),
        KV(Option("treeType"), Option("nodeTreePerRow")))
      })
    )
    )
  )

  final val recurringDependencyBreakTimes: Param[Int] = new Param[Int](this, "recurringDependencyBreakTimes",
    FormParams.toJson(Text(
      name = "recurringDependencyBreakTimes",
      value = "",
      extra = Extra(
        doc =
          """
            | default:1000  the max level should lower than this value;
            | When travel a tree, once a node is found two times, then the subtree will be ignore
          """,
        label = "Recurring dependency break times",
        options = Map(
          "valueType" -> "int",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )
}

case class IDParentID(id: Any, parentID: Any, children: scala.collection.mutable.ArrayBuffer[IDParentID])

