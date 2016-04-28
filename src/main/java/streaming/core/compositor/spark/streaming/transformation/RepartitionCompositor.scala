package streaming.core.compositor.spark.streaming.transformation

import java.util

import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.DStream
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
  * 4/28/16 WilliamZhu(allwefantasy@gmail.com)
  */
class RepartitionCompositor[T, S: ClassTag, U: ClassTag] extends Compositor[T] with CompositorHelper{

   protected var _configParams: util.List[util.Map[Any, Any]] = _

   val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

   override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
     this._configParams = configParams
   }

   def num = {
     config[Int]("num",_configParams)
   }

   override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
     val dstream = middleResult(0).asInstanceOf[DStream[S]]
     val _num = num.get
     val newDstream = dstream.repartition(_num)
     List(newDstream.asInstanceOf[T])
   }

 }
