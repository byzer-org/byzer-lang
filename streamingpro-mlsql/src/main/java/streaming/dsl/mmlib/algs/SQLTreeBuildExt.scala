package streaming.dsl.mmlib.algs

import net.liftweb.json.NoTypeHints
import net.liftweb.{json => SJSon}
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions => F}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

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
    val items = df.select($(idCol), $(parentIdCol)).distinct().rdd.map { row => IDParentID(row.get(0), row.get(1), ArrayBuffer()) }.collect()
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
    val newdf = df.sparkSession.read.json(rdd)


    val computeLevel1 = (a: Seq[Row], level: Int) => {
      val computeLevel = new ((Seq[Row], Int) => Int) {
        def apply(a: Seq[Row], level: Int): Int = {
          if (a.size == 0) return level;
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

    newdf.withColumn("level", computeLevelUDF(F.col("children"), F.lit(0)))
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new RuntimeException("register is not support by this estimator/transformer")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new RuntimeException("register is not support by this estimator/transformer")
  }

  final val idCol: Param[String] = new Param[String](this, "idCol", "")
  final val parentIdCol: Param[String] = new Param[String](this, "parentIdCol", "")
  final val topLevelMark: Param[String] = new Param[String](this, "topLevelMark", "")
}

case class IDParentID(id: Any, parentID: Any, children: scala.collection.mutable.ArrayBuffer[IDParentID])

