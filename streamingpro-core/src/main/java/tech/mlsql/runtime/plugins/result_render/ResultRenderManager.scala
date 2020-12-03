package tech.mlsql.runtime.plugins.result_render

import tech.mlsql.app.{ResultRender, ResultResp}
import tech.mlsql.runtime.AppRuntimeStore

object ResultRenderManager {

//  AppRuntimeStore.store.registerResultRender("EchoResultRender", classOf[EchoResultRender].getName)

  def call(req: ResultResp): ResultResp = {
    var target: ResultResp = req
    AppRuntimeStore.store.getResultRenders().foreach { exRender =>
      val rr = Class.forName(exRender.customClassItem.className).newInstance().asInstanceOf[ResultRender]
      target = rr.call(target)
    }
    target
  }
}

//class EchoResultRender extends ResultRender {
//  override def call(d: ResultResp): ResultResp = d
//}
