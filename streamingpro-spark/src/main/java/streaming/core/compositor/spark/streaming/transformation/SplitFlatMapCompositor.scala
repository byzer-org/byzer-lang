package streaming.core.compositor.spark.streaming.transformation

import java.util

import streaming.core.compositor.spark.streaming.CompositorHelper

/**
  * Created by gbshine on 2016/5/2.
  */
class SplitFlatMapCompositor[T] extends BaseFlatMapCompositor[T, String, String] with CompositorHelper {

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def sep = {
    config[String]("separator", _configParams)
  }

  override def flatMap: String => Iterable[String] = {
    val separator: String = sep.get
    x => x.split(separator)
  }
}
