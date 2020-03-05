package tech.mlsql.runtime.plugins.request_cleaner

import tech.mlsql.app.RequestCleaner
import tech.mlsql.runtime.AppRuntimeStore

/**
 * 9/1/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object RequestCleanerManager {
  def call() = {
    AppRuntimeStore.store.getRequestCleaners().foreach { cleaner =>
      Class.forName(cleaner.customClassItem.className).newInstance().asInstanceOf[RequestCleaner].call()
    }
  }
}
