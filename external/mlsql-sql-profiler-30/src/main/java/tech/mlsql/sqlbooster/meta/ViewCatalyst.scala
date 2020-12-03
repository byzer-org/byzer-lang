package tech.mlsql.sqlbooster.meta

/**
 * 2019-07-11 WilliamZhu(allwefantasy@gmail.com)
 */

case class TableHolder(tableWithDB: String, dbType: String)

trait ViewCatalyst {
  def register(name: String, tableWithDB: String, dbType: String): ViewCatalyst

  def getTableNameByViewName(name: String): TableHolder

}

class SimpleViewCatalyst extends ViewCatalyst {
  private val mapping = new java.util.concurrent.ConcurrentHashMap[String, TableHolder]()

  def register(name: String, tableWithDB: String, dbType: String): ViewCatalyst = {
    mapping.put(name, TableHolder(tableWithDB, dbType))
    this
  }

  def getTableNameByViewName(name: String): TableHolder = {
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
