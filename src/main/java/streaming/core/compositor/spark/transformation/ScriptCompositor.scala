package streaming.core.compositor.spark.transformation

import java.util

import net.liftweb.{json => SJSon}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.util.ScalaSourceCodeCompiler
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper

import scala.collection.JavaConversions._

/**
 * 8/2/16 WilliamZhu(allwefantasy@gmail.com)
 */
class ScriptCompositor[T] extends Compositor[T] with CompositorHelper {
  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }


  def scripts = {
    _configParams.get(1).map { fieldAndCode =>
      (fieldAndCode._1.toString, fieldAndCode._2 match {
        case a: util.List[String] => a.mkString(" ")
        case a: String => a
        case _ => ""
      })
    }
  }

  val TABLE = "_table_"
  val FUNC = "_func_"

  def outputTableName = {
    config[String]("outputTableName", _configParams)
  }

  def inputTableName = {
    config[String]("inputTableName", _configParams)
  }

  def source = {
    config[String]("source", _configParams)
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {

    if (params.containsKey(TABLE)) {
      //parent compositor is  tableCompositor

      val func = params.get(TABLE).asInstanceOf[(Any) => SQLContext]
      params.put(FUNC, (rddOrDF: Any) => {
        val sqlContext = func(rddOrDF)
        val newDF = createDF(params, sqlContext.table(inputTableName.get))
        outputTableName match {
          case Some(tableName) =>
            newDF.registerTempTable(tableName)
          case None =>
        }
        newDF
      })

    } else {
      // if not ,parent is SQLCompositor
      val func = params.get(FUNC).asInstanceOf[(DataFrame) => DataFrame]
      params.put(FUNC, (df: DataFrame) => {
        val newDF = createDF(params, func(df).sqlContext.table(inputTableName.get))
        outputTableName match {
          case Some(tableName) =>
            newDF.registerTempTable(tableName)
          case None =>
        }
        newDF
      })
    }
    params.remove(TABLE)

    middleResult

  }

  def createDF(params: util.Map[Any, Any], df: DataFrame) = {
    val _script = scripts
    val _source = source.getOrElse("")

    val _maps = _script.map { fieldAndCode =>
      var scriptCode = fieldAndCode._2
      if ("file" == _source || fieldAndCode._2.startsWith("file:/") || fieldAndCode._2.startsWith("hdfs:/")) {
        scriptCode = df.sqlContext.sparkContext.textFile(fieldAndCode._2).collect().mkString("\n")
      }
      (fieldAndCode._1, scriptCode)
    }

    val jsonRdd = df.rdd.map { row =>
      val maps = _maps.flatMap { f =>
        val executor = ScalaSourceCodeCompiler.execute(f._2)
        executor.execute(row.getAs[String](f._1))
      }

      val newMaps = row.schema.fieldNames.map(f => (f, row.getAs(f))).toMap ++ maps
      implicit val formats = SJSon.Serialization.formats(SJSon.NoTypeHints)
      SJSon.Serialization.write(newMaps)

    }

    df.sqlContext.read.json(jsonRdd)
  }

}
