package tech.mlsql.indexer.impl

import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, Cast, EqualTo, Expression, GreaterThanOrEqual, LessThanOrEqual, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, functions => F}
import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.utils.Md5
import tech.mlsql.indexer.MLSQLIndexer
import tech.mlsql.tool.LPUtils.splitConjunctivePredicates
import tech.mlsql.tool.{LPUtils, ZOrderingBytesUtil}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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
    val attributeToTableMapping = mutable.HashMap[AttributeReference, String]()
    val nameToFiledMapping = mutable.HashMap[String, AttributeReference]()
    val tableWithColumns = LPUtils.getTableAndColumns(lp)
    tableWithColumns.foreach { case (tableName, columns) =>
      columns.foreach { col =>
        attributeToTableMapping(col) = tableName
        nameToFiledMapping(s"${tableName}#${col.name}") = col
      }
    }

    val tableWithSchema = LPUtils.getTableAndSchema(lp)
    val zOrderingInfos = tableWithSchema.filter { case (table, schema) =>
      schema.fields.filter(_.name.startsWith("__mlsql_indexer_zordering")).size > 0
    }.map { case (table, schema) =>
      val meta = schema.fields.filter(_.name.startsWith("__mlsql_indexer_zordering")).head.metadata
      val fields = meta.getStringArray("indexFields")
      //val nameToFiledMapping = schema.fields.map(item => (item.name, item))
      val zOrderingFields = fields.map(f => nameToFiledMapping(s"${table}#${f}")).map(item => ZOrderingField(item, meta))
      ZOrderingInfo(table, zOrderingFields)
    }

    if (zOrderingInfos.size == 0) return lp

    //找到过滤条件，只关注and条件，处理完成后合并回去
    val newlp = lp transform {
      case Filter(condition: Expression, child: LogicalPlan) =>
        //展开所有and条件，获取所有属性
        val ands = splitConjunctivePredicates(condition)

        val targetFields = ArrayBuffer[ZOrderingTargetField]();

        //先抽取equal，必须是attributeRef + literal 且是在索引字段里的。缺失的用最大最小值补上。
        //最后转化为z-ordering值 然后加一个and条件放到第一个位置
        ands.foreach { item =>
          item match {
            case EqualTo(left@AttributeReference(name, _, _, _), right@Cast(Literal(v, _), _, _)) =>
              targetFields += ZOrderingTargetField(left, v)
            case EqualTo(left@AttributeReference(name, _, _, _), Literal(v, _)) =>
              targetFields += ZOrderingTargetField(left, v)

            case EqualTo(left@Cast(Literal(v, _), _, _), right@AttributeReference(name, _, _, _)) =>
              targetFields += ZOrderingTargetField(right, v)

            case EqualTo(Literal(v, _), right@AttributeReference(name, _, _, _)) =>
              targetFields += ZOrderingTargetField(right, v)
            case _ =>

          }
        }

        val newAnds = zOrderingInfos.map { info =>
          //构建索引查询
          val fieldsWithValue = info.fields.map { item =>
            val targetField = targetFields.filter(tf => tf.attribute.exprId == item.attribute.exprId).headOption
            targetField.map { tf =>
              val v = tf.attribute.dataType match {
                case LongType =>
                  tf.value.toString.toLong
                case DoubleType =>
                  tf.value.toString.toDouble

                case IntegerType =>
                  tf.value.toString.toLong

                case FloatType =>
                  tf.value.toString.toDouble

                case StringType =>
                  tf.value.toString
              }
              ZOrderingFieldWithMinMax(tf.attribute, v, v)
            }.
              getOrElse {

                val (min, max) = item.attribute.dataType match {
                  case LongType =>
                    (item.meta.getLong("min_" + item.attribute.name),
                      item.meta.getLong("max_" + item.attribute.name)
                    )
                  case DoubleType =>
                    (item.meta.getDouble("min_" + item.attribute.name),
                      item.meta.getDouble("max_" + item.attribute.name)
                    )

                  case IntegerType =>
                    (item.meta.getLong("min_" + item.attribute.name),
                      item.meta.getLong("max_" + item.attribute.name)
                    )

                  case FloatType =>
                    (item.meta.getDouble("min_" + item.attribute.name),
                      item.meta.getDouble("max_" + item.attribute.name)
                    )

                  case StringType =>
                    (item.meta.getString("min_" + item.attribute.name),
                      item.meta.getString("max_" + item.attribute.name)
                    )

                }
                ZOrderingFieldWithMinMax(item.attribute, min, max)
              }
          }
          val minMaxBytesZip = fieldsWithValue.map { item =>
            item.attribute.dataType match {
              case LongType =>
                (
                  ZOrderingBytesUtil.longTo8Byte(item.min.asInstanceOf[Long]),
                  ZOrderingBytesUtil.longTo8Byte(item.max.asInstanceOf[Long])
                )
              case DoubleType =>
                (
                  ZOrderingBytesUtil.doubleTo8Byte(item.min.asInstanceOf[Double]),
                  ZOrderingBytesUtil.doubleTo8Byte(item.max.asInstanceOf[Double])
                )

              case IntegerType =>
                (
                  ZOrderingBytesUtil.longTo8Byte(item.min.asInstanceOf[Long]),
                  ZOrderingBytesUtil.longTo8Byte(item.max.asInstanceOf[Long])
                )

              case FloatType =>
                (
                  ZOrderingBytesUtil.doubleTo8Byte(item.min.asInstanceOf[Double]),
                  ZOrderingBytesUtil.doubleTo8Byte(item.max.asInstanceOf[Double])
                )

              case StringType =>
                (
                  ZOrderingBytesUtil.utf8To8Byte(item.min.asInstanceOf[String]),
                  ZOrderingBytesUtil.utf8To8Byte(item.max.asInstanceOf[String])
                )

            }
          }
          val indexerAttr = tableWithColumns(info.table).filter(_.name.startsWith("__mlsql_indexer_zordering")).head
          val minBytes = ZOrderingBytesUtil.interleaveMulti8Byte(minMaxBytesZip.map(_._1).toArray)
          val maxBytes = ZOrderingBytesUtil.interleaveMulti8Byte(minMaxBytesZip.map(_._2).toArray)
          And(LessThanOrEqual(indexerAttr, Literal(maxBytes, BinaryType)), LessThanOrEqual(Literal(minBytes, BinaryType), indexerAttr))
        }
        Filter((newAnds ++ ands).reduce(And), child)
//        Filter((newAnds ).reduce(And), child)
    }

    newlp
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
          metabuilder.putLong(s"min_${field._2.name}", minMax(0).asInstanceOf[Int].toLong)
          metabuilder.putLong(s"max_${field._2.name}", minMax(1).asInstanceOf[Int].toLong)
        case FloatType =>
          metabuilder.putDouble(s"min_${field._2.name}", minMax(0).asInstanceOf[Float].toDouble)
          metabuilder.putDouble(s"max_${field._2.name}", minMax(1).asInstanceOf[Float].toDouble)
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
    //newDF.repartitionByRange(F.col(s"__mlsql_indexer_zordering_${newFiledName}"))
    Option(newDF)
  }
}

case class ZOrderingBinarySort(v: Array[Byte]) extends Ordered[ZOrderingBinarySort] with Serializable {
  override def compare(that: ZOrderingBinarySort): Int = {
    val len = this.v.length
    ZOrderingBytesUtil.compareTo(this.v, 0, len, that.v, 0, len)
  }
}


case class ZOrderingField(attribute: AttributeReference, meta: Metadata)

case class ZOrderingFieldWithMinMax(attribute: AttributeReference, min: Any, max: Any)

case class ZOrderingInfo(table: String, fields: Seq[ZOrderingField])

case class ZOrderingTargetField(attribute: AttributeReference, value: Any)
