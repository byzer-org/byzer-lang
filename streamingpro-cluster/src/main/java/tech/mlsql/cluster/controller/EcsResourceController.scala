package tech.mlsql.cluster.controller

import net.csdn.annotation.rest.At
import net.csdn.modules.http.ApplicationController
import net.csdn.modules.http.RestRequest.Method.{GET, POST}
import tech.mlsql.cluster.model.{EcsResourcePool, ElasticMonitor}

import scala.collection.JavaConverters._

/**
  * 2018-12-05 WilliamZhu(allwefantasy@gmail.com)
  */
class EcsResourceController extends ApplicationController {
  @At(path = Array("/ecs/add"), types = Array(GET, POST))
  def ecsAdd = {
    EcsResourcePool.requiredFields().asScala.foreach(item => require(hasParam(item), s"${item} is required"))
    EcsResourcePool.newOne(params())
    render(map("msg", "success"))
  }

  @At(path = Array("/ecs/remove"), types = Array(GET, POST))
  def ecsRemove = {
    val backend = EcsResourcePool.find(paramAsInt("id"))
    backend.delete()
    render(map("msg", "success"))
  }

  @At(path = Array("/ecs/list"), types = Array(GET, POST))
  def ecsList = {
    val items = EcsResourcePool.items()
    render(render(items))
  }

  @At(path = Array("/monitor/add"), types = Array(GET, POST))
  def monitorAdd = {
    ElasticMonitor.requiredFields().asScala.foreach(item => require(hasParam(item), s"${item} is required"))
    ElasticMonitor.newOne(params())
    render(map("msg", "success"))
  }
}
