package tech.mlsql.sqlbooster.meta

import tech.mlsql.dsl.adaptor.DslTool
import tech.mlsql.indexer.MlsqlOriTable

import scala.collection.JavaConverters._

/**
 * 2019-07-11 WilliamZhu(allwefantasy@gmail.com)
 */

trait ViewCatalyst {
  def register(name: String, tableWithDB: String, dbType: String, options: Map[String, String]): ViewCatalyst

  def getTableNameByViewName(name: String): MlsqlOriTable

  def values: List[MlsqlOriTable]

}

class SimpleViewCatalyst extends ViewCatalyst with DslTool {
  private val mapping = new java.util.concurrent.ConcurrentHashMap[String, MlsqlOriTable]()

  override def register(name: String, path: String, format: String,options: Map[String, String]): ViewCatalyst = {
    mapping.put(name, MlsqlOriTable(name, format, cleanStr(path), "",options))
    this
  }

  override def getTableNameByViewName(name: String): MlsqlOriTable = {
    mapping.get(name)
  }

  override def values: List[MlsqlOriTable] = mapping.values().asScala.toList

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
