package streaming.core.compositor.spark.transformation

import java.util

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper

import scala.collection.JavaConversions._

/**
  * 8/2/16 WilliamZhu(allwefantasy@gmail.com)
  */
class RowNumberCompositor[T] extends Compositor[T] with CompositorHelper {
  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def outputTableName = {
    config[String]("outputTableName", _configParams)
  }

  def inputTableName = {
    config[String]("inputTableName", _configParams)
  }

  def rankField = {
    config[String]("rankField", _configParams)
  }


  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {

    val context = sqlContextHolder(params)
    import org.apache.spark.sql.types.{LongType, StructField, StructType}

    val _inputTableName = inputTableName.get
    val _outputTableName = outputTableName.get
    val _rankField = rankField.get

    val table = context.table(_inputTableName)
    val schema = table.schema
    val rdd = table.rdd
    val newSchema = new StructType(schema.fields ++ Array(StructField(_rankField, LongType)))

    val newRowsWithScore = rdd.zipWithIndex().map { f =>
      org.apache.spark.sql.Row.fromSeq(f._1.toSeq ++ Array(f._2))
    }

    context.createDataFrame(newRowsWithScore, newSchema).registerTempTable(_outputTableName)

    middleResult

  }

}
