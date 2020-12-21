package tech.mlsql.sqlbooster.meta

import tech.mlsql.dsl.adaptor.DslTool
import tech.mlsql.indexer.MlsqlOriTable

/**
 * 2019-07-11 WilliamZhu(allwefantasy@gmail.com)
 */

trait ViewCatalyst {
  def register(name: String, tableWithDB: String, dbType: String): ViewCatalyst

  def getTableNameByViewName(name: String): MlsqlOriTable

}

class SimpleViewCatalyst extends ViewCatalyst with DslTool {
  private val mapping = new java.util.concurrent.ConcurrentHashMap[String, MlsqlOriTable]()

  def register(name: String, path: String, format: String): ViewCatalyst = {
    mapping.put(name, MlsqlOriTable(format, cleanStr(path), ""))
    this
  }

  def getTableNameByViewName(name: String): MlsqlOriTable = {
    mapping.get(name)
  }

}

object ViewCatalyst {
  private[this] val _context: ThreadLocal[ViewCatalyst] = new ThreadLocal[ViewCatalyst]

  def context(): ViewCatalyst = _context.get

  def createViewCatalyst(clzz: Option[String] = None) = {
    val temp = if (clzz.isDefined) Class.forName(clzz.get).newInstance().asInstanceOf[ViewCatalyst] else new SimpleViewCatalyst()
    _context.set(temp)
  }

  def meta = {
    _context.get()
  }

  def unset = _context.remove()
}
