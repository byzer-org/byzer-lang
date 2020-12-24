package tech.mlsql.runtime.metastore

import net.csdn.common.settings.ImmutableSettings
import net.csdn.modules.persist.mysql.MysqlClient
import net.sf.json.{JSONArray, JSONObject}
import org.apache.spark.sql.types.{MapType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions => F}
import tech.mlsql.common.utils.base.CaseFormat
import tech.mlsql.common.utils.names.NameConvert
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.runtime.MetaStoreService.ctx
import tech.mlsql.runtime.MetaStoreService.ctx._
import tech.mlsql.store.DictType.DictType
import tech.mlsql.store.{DBStore, WDictStore}

import scala.collection.JavaConverters._

/**
 * MySQLDBStore for now only support complex type like List[String],Map[String,String].
 */
class MySQLDBStore extends DBStore {

  import MySQLDBStore._


  override def readTable(spark: SparkSession, tableName: String): DataFrame = {
    val Array(db, table) = tableName.split("\\.")
    val client = new MysqlClient(ctx.dataSource)
    client.settings(ImmutableSettings.settingsBuilder().build())
    import spark.implicits._
    val db_table = "w_" + NameConvert.lowerCamelToLowerUnderScore(table)

    var data = spark.read.json(spark.createDataset[String](
      client.query(s"select * from $db_table").asScala.
        map { f =>
          val _item = f.asScala.map { item =>
            val value = item._2

            val newValue = value match {
              case a: String =>
                getJson(a)
              case _ => value
            }

            (NameConvert.lowerUnderScoreToLowerCamel(item._1.toString), newValue)

          }.toMap
          JSONTool.toJsonStr(_item)
        }))

    if (data.count() == 0) throw new RuntimeException(s"There is no data in ${tableName}")

    val convertRowToMap = (row: Row) => {
      row.schema.fieldNames.filter(field => !row.isNullAt(row.fieldIndex(field))).map(field => field -> row.getAs[String](field)).toMap
    }
    val udf = F.udf(convertRowToMap, MapType(StringType, StringType))
    data.schema.filter(f => f.dataType match {
      case StructType(_) => true
      case _ => false
    }).map { wow =>
      data = data.withColumn(wow.name, udf(F.col(wow.name)))
    }
    data
  }

  override def tryReadTable(spark: SparkSession, table: String, empty: () => DataFrame): DataFrame = {
    try {
      readTable(spark, table)
    } catch {
      case e: Exception =>
        empty()
    }
  }

  override def saveTable(spark: SparkSession, data: DataFrame, tableName: String, updateCol: Option[String], isDelete: Boolean): Unit = {
    val client = new MysqlClient(ctx.dataSource)
    client.settings(ImmutableSettings.settingsBuilder().build())
    val Array(db, table) = tableName.split("\\.")
    val db_table = "w_" + CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, table)
    val schema = data.schema.fields.map(_.name)
    val appRecords = data.toJSON.collect().toList
    appRecords.foreach { _item =>

      val item = JSONObject.fromObject(_item)

      updateCol match {
        case Some(idCols) =>
          val condition = idCols.split(",").map(name => s" ${NameConvert.lowerCamelToLowerUnderScore(name)}=? ").mkString("and")
          val condParams = idCols.split(",").map(name => item.get(name)).toList.asJava.toArray


          val updateCond = schema.map(name => s" ${NameConvert.lowerCamelToLowerUnderScore(name)}=? ").mkString(",")
          val updateParams = schema.map(name => cleanValue(item,name)).toList.asJava.toArray

          if (isDelete) {
            client.execute(s"delete from $db_table where ${condition}", condParams: _*)
          } else {
            val items = client.query(s"select * from $db_table where ${condition}", condParams: _*)
            if (items.size() > 0) {
              val params = (updateParams.toList ++ condParams.toList).toArray
              client.execute(s"update $db_table set ${updateCond} where ${condition}", params: _*)
            } else {
              val insertCond = schema.map(name => s" ${NameConvert.lowerCamelToLowerUnderScore(name)} ").mkString(",")
              val insertCond2 = schema.map(name => s" ? ").mkString(",")
              val insertParams = schema.map(name => cleanValue(item,name)).toList.asJava.toArray
              client.execute(s"INSERT INTO $db_table (${insertCond}) VALUES (${insertCond2})", insertParams: _*)
            }
          }

        case None =>
          val condition = schema.map(name => s" ${NameConvert.lowerCamelToLowerUnderScore(name)}=? ").mkString("and")
          val condParams = schema.map(name => item.get(name)).toList.asJava.toArray

          val insertCond = schema.map(name => s" ${NameConvert.lowerCamelToLowerUnderScore(name)} ").mkString(",")
          val insertCond2 = schema.map(name => s" ? ").mkString(",")
          val insertParams = schema.map { name =>
            cleanValue(item,name)
          }.toList.asJava.toArray

          if (isDelete) {
            client.execute(s"delete from $db_table where ${condition}", condParams: _*)
          } else {
            client.execute(s"INSERT INTO $db_table (${insertCond}) VALUES (${insertCond2})", insertParams: _*)
          }
      }

    }
  }

  def cleanValue(item: JSONObject, name: String) = {

    val value = item.get(name)
    if (value.isInstanceOf[JSONObject] || value.isInstanceOf[JSONArray]) {
      value.toString
    } else value

  }

  override def saveConfig(spark: SparkSession, appPrefix: String, name: String, value: String, dictType: DictType): Unit = {
    val key = s"${appPrefix}_${name}"
    ctx.run(ctx.query[WDictStore].insert(
      _.id -> lift(0),
      _.name -> lift(key),
      _.value -> lift(value),
      _.dictType -> lift(dictType.id))
    )
  }

  override def readConfig(spark: SparkSession, appPrefix: String, name: String, dictType: DictType): Option[WDictStore] = {
    val key = s"${appPrefix}_${name}"
    ctx.run(ctx.query[WDictStore].filter(_.name == lift(key)).filter(_.dictType == lift(dictType.id))).headOption
  }

  override def readAllConfig(spark: SparkSession, appPrefix: String): List[WDictStore] = {
    ctx.run(ctx.query[WDictStore]).filter(f => f.name.startsWith(appPrefix))
  }
}

object MySQLDBStore {

  def getJson(value: String): Any = {
    if (value.startsWith("[") && value.endsWith("]")) {
      return JSONTool.parseJson[List[String]](value)
    }
    if (value.startsWith("{") && value.endsWith("}")) {
      return JSONTool.parseJson[Map[String, String]](value)
    }
    return value
  }


  //  def wipeOutComplexStruct(schema: Array[String], _item: Row) = {
  //    val item = schema.map { name =>
  //      val index = schema.indexOf(name)
  //      val col = _item.get(index)
  //      if (col == null) null
  //      else {
  //        col match {
  //          case a: Long => a
  //          case a: Int => a
  //          case a: String => a
  //          case a: Date => a
  //          case a: java.sql.Date => a
  //          case a: Boolean => a
  //          case a: Double => a
  //          case a: Float => a
  //          case _ => try {
  //            WowJsonInferSchema
  //          } catch {
  //            case e: Exception => col
  //          }
  //        }
  //      }
  //
  //    }
  //    Row.fromSeq(item.toSeq)
  //  }

}
