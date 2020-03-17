package tech.mlsql.runtime.metastore

import java.util.Date

import net.csdn.common.settings.ImmutableSettings
import net.csdn.modules.persist.mysql.MysqlClient
import net.sf.json.{JSONArray, JSONObject}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.spark_project.guava.base.CaseFormat
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.runtime.MetaStoreService.ctx
import tech.mlsql.runtime.PluginUtils
import tech.mlsql.store.DBStore

import scala.collection.JavaConverters._

/**
 * not support scheduler yet
 */
class MySQLDBStore extends DBStore {

  import MySQLDBStore._

  override def readTable(spark: SparkSession, tableName: String): DataFrame = {
    val Array(db, table) = tableName.split("\\.")
    val client = new MysqlClient(ctx.dataSource)
    client.settings(ImmutableSettings.settingsBuilder().build())
    import spark.implicits._
    val db_table = "w_" + CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, table)

    val data = spark.read.json(spark.createDataset[String](
      client.query(s"select * from $db_table").asScala.
        map { f =>
          val _item = f.asScala.map { item =>
            val value = item._2

            val newValue = value match {
              case a: String =>
                getJson(a)
              case _ => value
            }

            (underScoreToCamel(item._1.toString), value)

          }.toMap
          JSONTool.toJsonStr(_item)
        }))

    if (data.count() == 0) throw new RuntimeException("no data")
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
    val appRecords = data.collect().toList
    appRecords.foreach { _item =>

      val item = MySQLDBStore.wipeOutComplexStruct(schema, _item)

      updateCol match {
        case Some(idCols) =>
          val condition = idCols.split(",").map(name => s" ${camelToUnderScore(name)}=? ").mkString("and")
          val condParams = idCols.split(",").map(name => item.get(schema.indexOf(name))).toList.asJava.toArray


          val updateCond = schema.map(name => s" ${camelToUnderScore(name)}=? ").mkString(",")
          val updateParams = schema.map(name => item.get(schema.indexOf(name))).toList.asJava.toArray

          if (isDelete) {
            client.execute(s"delete from $db_table where ${condition}", condParams: _*)
          } else {
            val items = client.query(s"select * from $db_table where ${condition}", condParams: _*)
            if (items.size() > 0) {
              val params = (updateParams.toList ++ condParams.toList).toArray
              client.execute(s"update $db_table set ${updateCond} where ${condition}", params: _*)
            } else {
              val insertCond = schema.map(name => s" ${camelToUnderScore(name)} ").mkString(",")
              val insertCond2 = schema.map(name => s" ? ").mkString(",")
              val insertParams = schema.map(name => item.get(schema.indexOf(name))).toList.asJava.toArray
              client.execute(s"INSERT INTO $db_table (${insertCond}) VALUES (${insertCond2})", insertParams: _*)
            }
          }

        case None =>
          val condition = schema.map(name => s" ${camelToUnderScore(name)}=? ").mkString("and")
          val condParams = schema.map(name => item.get(schema.indexOf(name))).toList.asJava.toArray

          val insertCond = schema.map(name => s" ${camelToUnderScore(name)} ").mkString(",")
          val insertCond2 = schema.map(name => s" ? ").mkString(",")
          val insertParams = schema.map(name => item.get(schema.indexOf(name))).toList.asJava.toArray

          if (isDelete) {
            client.execute(s"delete from $db_table where ${condition}", condParams: _*)
          } else {
            client.execute(s"INSERT INTO $db_table (${insertCond}) VALUES (${insertCond2})", insertParams: _*)
          }
      }

    }
  }
}

object MySQLDBStore {

  def getJson(value: String): Any = {
    if (value.startsWith("[") && value.endsWith("]")) {
      return JSONArray.fromObject(value)
    }
    if (value.startsWith("{") && value.endsWith("}")) {
      return JSONObject.fromObject(value)
    }
    return value
  }

  def camelToUnderScore(name: String) = {
    CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name)
  }

  def underScoreToCamel(name: String) = {
    CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, name)
  }

  def wipeOutComplexStruct(schema: Array[String], _item: Row) = {
    val item = schema.map { name =>
      val index = schema.indexOf(name)
      val col = _item.get(index)
      if (col == null) null
      else {
        col match {
          case a: Long => a
          case a: Int => a
          case a: String => a
          case a: Date => a
          case a: java.sql.Date => a
          case a: Boolean => a
          case a: Double => a
          case a: Float => a
          case _ => try {
            JSONTool.toJsonStr(col.asInstanceOf[AnyRef])
          } catch {
            case e: Exception => col
          }
        }
      }

    }
    Row.fromSeq(item.toSeq)
  }
}
