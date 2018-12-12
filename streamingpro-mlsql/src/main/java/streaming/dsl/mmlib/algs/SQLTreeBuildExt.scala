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
          return a.map { row =>
            val index = a.head.schema.zipWithIndex.filter(s => s._1.name == "children").head._2
            val value = row.getSeq[Row](index)
            apply(value, level + 1)
          }.max

        }
      }
      computeLevel(a, level)
    }
    val computeLevelUDF = F.udf(computeLevel1, IntegerType)
    newdf = newdf.withColumn("level", computeLevelUDF(F.col("children"), F.lit(0)))

    val idToItem = items.map(f => (f.id, f)).toMap
    //    val ROOTSBr = df.sparkSession.sparkContext.broadcast(ROOTS)
    //    val idToItemBr = df.sparkSession.sparkContext.broadcast(idToItem)

    //    val fetchTopID = (item: IDParentID) => {
    //      var res = item.id
    //      var stopCond = 100000
    //      while (idToItemBr.value(res).parentID != null && idToItemBr.value(res).parentID != t && stopCond > 0) {
    //        res = idToItemBr.value(res).parentID
    //        stopCond -= 1
    //      }
    //      res
    //    }

    $(treeType) match {
      case "treePerRow" => newdf
      case "nodeTreePerRow" =>
        val rdd = df.sparkSession.sparkContext.parallelize(items.toSeq).map { item =>
          //find the top of one tree
          //          val topTree = ROOTSBr.value.filter(f => f.id == fetchTopID(item)).head

          //          // find the subTree of some item
          //          val subTree = new ((IDParentID) => Option[IDParentID]) {
          //            def apply(a: IDParentID): Option[IDParentID] = {
          //              if (a.id == item.id) {
          //                Option(a)
          //              } else {
          //                a.children.map(sub => apply(sub)).filter(f => f.isDefined).map(f => f.get).headOption
          //              }
          //            }
          //          }
          //          val computeTree = subTree(topTree)

          // collect all items of subTree
          val resultset = new mutable.HashSet[IDParentID]()
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
          if (item.children.size > 0) {
            collectAll(item)
          }
          Row.fromSeq(Seq(item.id.toString, resultset.map(f => f.id.toString).toSeq))
        }
        df.sparkSession.createDataFrame(rdd, StructType(Seq(
          StructField(name = "id", dataType = StringType), StructField(name = "children", dataType = ArrayType(StringType))
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
      |  TreeBuildExt used to build a tree when you have father - child relationship, please
      |  check the codeExample to see how to use it.
    """.stripMargin)


  override def codeExample: Code = Code(SQLCode, CodeExampleText.jsonStr +
    """
      |set jsonStr = '''
      |{"id":0,"parentId":null}
      |{"id":1,"parentId":null}
      |{"id":2,"parentId":1}
      |{"id":3,"parentId":3}
      |{"id":7,"parentId":0}
      |{"id":199,"parentId":1}
      |{"id":200,"parentId":199}
      |''';
      |
      |load jsonStr.`jsonStr` as data;
      |run data as TreeBuildExt.`` where idCol="id" and parentIdCol="parentId" and treeType="nodeTreePerRow" as result;
      |
      |here are the result:
      |
      |+---+-------------+
      ||id |children     |
      |+---+-------------+
      ||200|[200]        |
      ||0  |[7]          |
      ||1  |[200, 2, 199]|
      ||7  |[7]          |
      ||199|[200]        |
      ||2  |[2]          |
      |+---+-------------+
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
    """.stripMargin)

  override def coreCompatibility: Seq[CoreVersion] = super.coreCompatibility

  final val idCol: Param[String] = new Param[String](this, "idCol", "")
  final val parentIdCol: Param[String] = new Param[String](this, "parentIdCol", "")
  final val topLevelMark: Param[String] = new Param[String](this, "topLevelMark", "")
  final val treeType: Param[String] = new Param[String](this, "treeType", "treePerRow|nodeTreePerRow")
}

case class IDParentID(id: Any, parentID: Any, children: scala.collection.mutable.ArrayBuffer[IDParentID])

