package streaming.core.compositor.spark.transformation

import _root_.streaming.core.{CompositorHelper, StreamingproJobManager, StreamingproJobType}
import _root_.streaming.dsl.{ScriptSQLExec, ScriptSQLExecListener}
import org.apache.log4j.Logger
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import java.util

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 5/7/2018.
  */
class MLSQLCompositor[T] extends Compositor[T] with CompositorHelper {
  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def sql = {
    _configParams.get(0).get("sql") match {
      case a: util.List[String] => Some(a.mkString(" "))
      case a: String => Some(a)
      case _ => None
    }
  }


  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    StreamingproJobManager.init(sparkSession(params).sparkContext)
    require(sql.isDefined, "please set sql  by variable `sql` in config file")

    val _sql = translateSQL(sql.get, params)

    val jobInfo = StreamingproJobManager.getStreamingproJobInfo(
      "admin", StreamingproJobType.SCRIPT, "", _sql,
      -1L
    )
    StreamingproJobManager.run(sparkSession(params), jobInfo, () => {
      val context = new ScriptSQLExecListener(sparkSession(params), "", Map())
      ScriptSQLExec.parse(_sql, context)
    })
    StreamingproJobManager.shutdown
    if (middleResult == null) List() else middleResult
  }
}
