package tech.mlsql.runtime.plugins.result_render

import tech.mlsql.app.{ResultRender, ResultResp}
import tech.mlsql.runtime.AppRuntimeStore

object ResultRenderManager {
  def call(req: ResultResp): ResultResp = {
    var target: ResultResp = req
    AppRuntimeStore.store.getResultRenders().foreach { exRender =>
      val rr = Class.forName(exRender.customClassItem.className).newInstance().asInstanceOf[ResultRender]
      target = rr.call(target)
    }
    target
  }
}

