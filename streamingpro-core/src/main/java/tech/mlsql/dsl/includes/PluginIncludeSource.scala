package tech.mlsql.dsl.includes

import java.util
import java.util.jar.JarFile

import net.csdn.common.settings.ImmutableSettings.settingsBuilder
import net.csdn.common.settings.Settings
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import serviceframework.dispatcher.{Compositor, Processor, Strategy, StrategyDispatcher}
import streaming.core.CompositorHelper
import streaming.core.strategy.{DebugTrait, JobStrategy, ParamsValidator}
import streaming.dsl.IncludeSource
import tech.mlsql.common.utils.log.Logging

import scala.collection.JavaConverters._
import scala.io.Source

/**
  * 2019-09-16 WilliamZhu(allwefantasy@gmail.com)
  */
class PluginIncludeSource extends IncludeSource with Logging {

  import PluginIncludeSource._

  /**
    * include plugin.`binlog2delta`;
    */
  override def fetchSource(sparkSession: SparkSession, path: String, options: Map[String, String]): String = {

    val (pluginName, scriptPath) = path.split("/", 2) match {
      case Array(pluginName, scriptPath) => (pluginName, scriptPath)
      case Array(pluginName) => (pluginName, null)
    }

    require(store.containsKey(pluginName), s"plugin ${pluginName} is not installed.")

    val jarFile = new JarFile(store.get(pluginName))

    val file2Content = jarFile.entries().asScala.map { item =>
      if (!item.isDirectory && !item.getName.endsWith(".class")) {
        (item.getName, Source.fromInputStream(jarFile.getInputStream(item), "utf-8").getLines().mkString("\n"))
      } else null
    }.filter(_ != null).toMap

    require(file2Content.get("plugin.json").isDefined, "invalid script plugin")

    if (scriptPath != null) {
      return file2Content(scriptPath)
    }

    val settings: Settings = settingsBuilder.build()
    val temp = new StrategyDispatcher[Any](settings)
    temp.loadConfig(file2Content("plugin.json"))
    val entry = temp.findStrategies(path).get.head.configParams.get("entry").toString
    file2Content(entry)
  }

  override def skipPathPrefix: Boolean = true
}

object PluginIncludeSource {
  private val store = new java.util.concurrent.ConcurrentHashMap[String, String]()

  def register(pluginName: String, localJarPath: String): Unit = {
    store.put(pluginName, localJarPath)
  }

  def unRegister(pluginName: String): Unit = {
    store.remove(pluginName)
  }

}

class Step[T] extends Compositor[T] with CompositorHelper {
  var _configParams: util.List[util.Map[Any, Any]] = _

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    middleResult
  }
}

class DefaultStrategy[T] extends Strategy[T] with DebugTrait with JobStrategy {

  import scala.collection.JavaConversions._

  var _name: String = _
  var _ref: util.List[Strategy[T]] = _
  var _compositor: util.List[Compositor[T]] = _
  var _processor: util.List[Processor[T]] = _
  var _configParams: util.Map[Any, Any] = _

  val logger = Logger.getLogger(getClass.getName)

  def processor: util.List[Processor[T]] = _processor

  def ref: util.List[Strategy[T]] = _ref

  def compositor: util.List[Compositor[T]] = _compositor

  def name: String = _name

  def initialize(name: String, alg: util.List[Processor[T]], ref: util.List[Strategy[T]], com: util.List[Compositor[T]], params: util.Map[Any, Any]): Unit = {
    this._name = name
    this._ref = ref
    this._compositor = com
    this._processor = alg
    this._configParams = params

  }

  def result(params: util.Map[Any, Any]): util.List[T] = {

    val validateResult = compositor.filter(f => f.isInstanceOf[ParamsValidator]).map { f =>
      f.asInstanceOf[ParamsValidator].valid(params)
    }.filterNot(f => f._1)

    if (validateResult.size > 0) {
      throw new IllegalArgumentException(validateResult.map(f => f._2).mkString("\n"))
    }

    ref.foreach { r =>
      r.result(params)
    }

    if (compositor != null && compositor.size() > 0) {
      var middleR = compositor.get(0).result(processor, ref, null, params)
      for (i <- 1 until compositor.size()) {
        middleR = compositor.get(i).result(processor, ref, middleR, params)
      }
      middleR
    } else {
      //processor.get(0).result(params)
      new util.ArrayList[T]()
    }


  }


  def configParams: util.Map[Any, Any] = _configParams
}
