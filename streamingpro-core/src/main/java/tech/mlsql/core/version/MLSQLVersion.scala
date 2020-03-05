package tech.mlsql.core.version

import java.io.{IOException, InputStream}
import java.util.Properties

/**
  * 2019-04-07 WilliamZhu(allwefantasy@gmail.com)
  */
object MLSQLVersion {

  private val versionFile: String = "mlsql-version-info.properties"

  private val info = load()

  protected def _getVersion: String = info.getProperty("version", "Unknown")

  protected def _getDate: String = info.getProperty("date", "Unknown")

  protected def _getUser: String = info.getProperty("user", "Unknown")

  protected def _getSrcChecksum: String = info.getProperty("srcChecksum", "Unknown")

  protected def _getRevision: String = info.getProperty("revision", "Unknown")

  protected def _getBranch: String = info.getProperty("branch", "Unknown")

  protected def _getUrl: String = info.getProperty("url", "Unknown")


  def version() = {
    VersionInfo(_getVersion, _getUser, _getDate, _getSrcChecksum, _getRevision, _getBranch, _getUrl)
  }

  def load(): Properties = {
    val info = new Properties
    var is: InputStream = null
    try {
      is = getResourceAsStream(versionFile)
      info.load(is)
    } catch {
      case ex: IOException =>
    } finally {
      if (is != null) try
        is.close()
      catch {
        case e: IOException =>
          e.printStackTrace()
      }
    }
    info
  }

  import java.io.IOException

  @throws[IOException]
  def getResourceAsStream(resourceName: String): InputStream = {
    val cl = Thread.currentThread.getContextClassLoader
    if (cl == null) throw new IOException("Can not read resource file '" + resourceName + "' because class loader of the current thread is null")
    getResourceAsStream(cl, resourceName)
  }

  @throws[IOException]
  def getResourceAsStream(cl: ClassLoader, resourceName: String): InputStream = {
    if (cl == null) throw new IOException("Can not read resource file '" + resourceName + "' because given class loader is null")
    val is = cl.getResourceAsStream(resourceName)
    if (is == null) throw new IOException("Can not read resource file '" + resourceName + "'")
    is
  }
}

case class VersionInfo(version: String, buildBy: String, date: String, srcChecksum: String, revision: String, branch: String, url: String)
