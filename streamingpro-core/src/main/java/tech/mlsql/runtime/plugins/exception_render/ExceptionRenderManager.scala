package tech.mlsql.runtime.plugins.exception_render

import tech.mlsql.app.{ExceptionRender, ExceptionResult}
import tech.mlsql.runtime.AppRuntimeStore

object ExceptionRenderManager {

  AppRuntimeStore.store.registerExceptionRender("ArrowExceptionRender", classOf[ArrowExceptionRender].getName)

  def call(e: Exception): ExceptionResult = {
    var meet = false
    var target: ExceptionResult = null
    AppRuntimeStore.store.getExceptionRenders().foreach { exRender =>
      if (!meet) {
        val item = Class.forName(exRender.customClassItem.className).newInstance().asInstanceOf[ExceptionRender].call(e: Exception)
        if (item.str.isDefined) {
          meet = true
          target = item
        }
      }
    }
    if (meet) target else new DefaultExceptionRender().call(e)
  }
}
