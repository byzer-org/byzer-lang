package tech.mlsql.indexer.impl

import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, functions => F}
import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.utils.Md5
import tech.mlsql.indexer.MLSQLIndexer
import tech.mlsql.tool.{LPUtils, ZOrderingBytesUtil}

/**
 * 31/12/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class ZOrderingIndexer extends MLSQLIndexer {
  private val context = ScriptSQLExec.context()
  private val session = context.execListener.sparkSession

  override def rewrite(lp: LogicalPlan, options: Map[String, String]): LogicalPlan = {
    //    val schema = lp.schema
    //    val columnsMap = schema.fields.map(item => (item.name, item)).toMap

    //获取涉及的表以及对应的列，检查这些表是不是有__mlsql_indexer_zordering开头的列。
    // 并把这些列提取出来
    val tableWithColumns = LPUtils.getTableAndColumns(lp)
    val tableWithSchema = LPUtils.getTableAndSchema(lp)
    val tablesWithZOrderingIndexer = tableWithSchema.filter { case (table, schema) =>
      schema.fields.filter(_.name.startsWith("__mlsql_indexer_zordering")).size > 0
    }

    if (tablesWithZOrderingIndexer.size == 0) return lp

    lp.transform {
      case a@Filter(cond, child) =>

        a
    }

    lp
  }

  override def read(sql: LogicalPlan, options: Map[String, String]): Option[DataFrame] = ???

  override def write(df: DataFrame, options: Map[String, String]): Option[DataFrame] = {
    if (!options.contains("indexFields")) return Option(df)
    val columnsMap = df.schema.fields.map(item => (item.name, item)).toMap
    val indexFields = options.
      get("indexFields").
      map(_.split(",")).
      getOrElse(Array[String]()).map { item =>
      val newitem = columnsMap(item)
      (df.schema.fields.indexOf(newitem), newitem)
    }
    val fieldNum = df.schema.fields.length
    val newRDD = df.rdd.map { row =>
      val values = indexFields.map { case (index, field) =>
        field.dataType match {
          case LongType =>
            ZOrderingBytesUtil.longTo8Byte(row.getLong(index))
          case DoubleType =>
            ZOrderingBytesUtil.doubleTo8Byte(row.getDouble(index))
          case IntegerType =>
            ZOrderingBytesUtil.intTo8Byte(row.getInt(index))
          case FloatType =>
            ZOrderingBytesUtil.doubleTo8Byte(row.getFloat(index).toDouble)
          case StringType =>
            ZOrderingBytesUtil.utf8To8Byte(row.getString(index))
        }
      }
      val zOrderingValue = ZOrderingBytesUtil.interleaveMulti8Byte(values)
      Row.fromSeq(row.toSeq ++ Seq(zOrderingValue))
    }.sortBy(x => ZOrderingBinarySort(x.getAs[Array[Byte]](fieldNum)))

    val newFiledName = Md5.md5Hash(indexFields.map(_._2.name).mkString(""))
    val metabuilder = new MetadataBuilder()

    //收集min/max值
    val values = indexFields.flatMap(item => Seq(F.min(F.col(item._2.name)), F.max(F.col(item._2.name))))
    val minMaxCols = df.agg(values(0), values.slice(1, values.length): _*).collect().head
    val colMinMaxGroups = minMaxCols.toSeq.grouped(2).toList
    indexFields.zip(colMinMaxGroups).foreach { case (field, minMax) =>
      field._2.dataType match {
        case LongType =>
          metabuilder.putLong(s"min_${field._2.name}", minMax(0).asInstanceOf[Long])
          metabuilder.putLong(s"max_${field._2.name}", minMax(1).asInstanceOf[Long])
        case DoubleType =>
          metabuilder.putDouble(s"min_${field._2.name}", minMax(0).asInstanceOf[Double])
          metabuilder.putDouble(s"max_${field._2.name}", minMax(1).asInstanceOf[Double])
        case IntegerType =>
          metabuilder.putLong(s"min_${field._2.name}", minMax(0).asInstanceOf[Int])
          metabuilder.putLong(s"max_${field._2.name}", minMax(1).asInstanceOf[Int])
        case FloatType =>
          metabuilder.putDouble(s"min_${field._2.name}", minMax(0).asInstanceOf[Float])
          metabuilder.putDouble(s"max_${field._2.name}", minMax(1).asInstanceOf[Float])
        case StringType =>
          metabuilder.putString(s"min_${field._2.name}", minMax(0).asInstanceOf[String])
          metabuilder.putString(s"max_${field._2.name}", minMax(1).asInstanceOf[String])
      }
    }

    val meta = metabuilder.putStringArray("indexFields", indexFields.map(_._2.name)).build()
    val newDF = df.sparkSession.createDataFrame(newRDD, StructType(
      df.schema.fields ++ Seq(
        StructField(s"__mlsql_indexer_zordering_${newFiledName}",
          BinaryType, false, meta))
    ))

    Option(newDF)
  }
}

case class ZOrderingBinarySort(v: Array[Byte]) extends Ordered[ZOrderingBinarySort] with Serializable {
  override def compare(that: ZOrderingBinarySort): Int = {
    val len = this.v.length
    ZOrderingBytesUtil.compareTo(this.v, 0, len, that.v, 0, len)
  }
}
