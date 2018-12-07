package tech.mlsql.cluster.controller

import net.csdn.annotation.rest.At
import net.csdn.modules.http.ApplicationController
import net.csdn.modules.http.RestRequest.Method.{GET, POST}
import net.liftweb.json.NoTypeHints
import net.liftweb.{json => SJSon}
import tech.mlsql.cluster.model.Backend
import tech.mlsql.cluster.service.BackendService

/**
  * 2018-12-05 WilliamZhu(allwefantasy@gmail.com)
  */
class BackendController extends ApplicationController {

  @At(path = Array("/backend/add"), types = Array(GET, POST))
  def backendAdd = {
    List("url", "tag", "name").foreach(item => require(hasParam(item), s"${item} is required"))
    Backend.newOne(params(),true)
    BackendService.refreshCache
    render(map("msg", "success"))
  }

  @At(path = Array("/backend/tags/update"), types = Array(GET, POST))
  def backendTagsUpdate = {
    val backend = Backend.find(paramAsInt("id"))
    if (param("merge", "overwrite") == "overwrite") {
      backend.attr("tag", param("tags"))
    } else {
      val newTags = backend.getTag.split(",").toSet ++ param("tags").split(",").toSet
      backend.attr("tag", newTags.mkString(","))
    }
    backend.save()
    BackendService.refreshCache
    render(map("msg", "success"))
  }

  @At(path = Array("/backend/remove"), types = Array(GET, POST))
  def backendRemove = {
    val backend = Backend.find(paramAsInt("id"))
    backend.delete()
    BackendService.refreshCache
    render(map("msg", "success"))
  }

  @At(path = Array("/backend/list"), types = Array(GET, POST))
  def backendList = {
    render(Backend.items())
  }

  @At(path = Array("/backend/active"), types = Array(GET, POST))
  def activeBackend = {
    implicit val formats = SJSon.Serialization.formats(NoTypeHints)
    render(SJSon.Serialization.write(BackendService.activeBackend.map(f => (f._1.getName, f._2))))
  }
}
