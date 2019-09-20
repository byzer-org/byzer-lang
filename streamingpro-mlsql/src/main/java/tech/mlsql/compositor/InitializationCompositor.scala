package tech.mlsql.compositor

/**
  * 2019-08-23 WilliamZhu(allwefantasy@gmail.com)
  */

import java.util

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.CompositorHelper
import streaming.core.compositor.spark.transformation.SQLCompositor
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import streaming.dsl.{MLSQLExecuteContext, ScriptSQLExec, ScriptSQLExecListener}
import tech.mlsql.ets.ScriptRunner
import tech.mlsql.job.{JobManager, MLSQLJobType}

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 5/7/2018.
  */
class InitializationCompositor[T] extends Compositor[T] with CompositorHelper {
  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def sql = {
    _configParams.get(0).get("mlsql") match {
      case a: util.List[_] => Some(a.mkString(" "))
      case a: String => Some(a)
      case _ => None
    }
  }

  def owner = {
    _configParams.get(0).get("owner") match {
      case a: String => Some(a)
      case _ => None
    }
  }

  def home = {
    _configParams.get(0).get("home") match {
      case a: String => Some(a)
      case _ => None
    }
  }

  def setUpSession(owner: String) = {
    if (owner != "admin")
      PlatformManager.getRuntime.asInstanceOf[SparkRuntime].getSession(owner)
    else PlatformManager.getRuntime.asInstanceOf[SparkRuntime].sparkSession
  }

  def setUpScriptSQLExecListener(owner: String, sparkSession: SparkSession, groupId: String) = {
    val context = new ScriptSQLExecListener(sparkSession, "", Map[String, String](owner -> home.getOrElse("")))
    ScriptSQLExec.setContext(new MLSQLExecuteContext(context, owner, context.pathPrefix(None), groupId, Map()))
    context.addEnv("SKIP_AUTH", "true")
    context.addEnv("HOME", context.pathPrefix(None))
    context.addEnv("OWNER", owner)
    context
  }

  /**
    *
    * this initial compositor should not use HOME
    */
  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    require(sql.isDefined, "please set sql  by variable `sql` in config file")

    val _sql = translateSQL(sql.get, params)
    val _owner = owner.getOrElse("admin")
    val session = setUpSession(_owner)
    val job = JobManager.getJobInfo(_owner,  MLSQLJobType.SCRIPT,"initial-job", _sql, -1)
    setUpScriptSQLExecListener(_owner, session, job.groupId)
    ScriptRunner.runJob(_sql, job, (df) => {
      df.show(100)
    })

    if (middleResult == null) List() else middleResult
  }
}
