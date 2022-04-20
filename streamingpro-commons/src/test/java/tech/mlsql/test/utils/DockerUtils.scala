package tech.mlsql.test.utils

import org.slf4j.{Logger, LoggerFactory}

import java.io.{File, IOException}

object DockerUtils {
  private val LOG: Logger = LoggerFactory.getLogger(DockerUtils.getClass)

  /**
   * Get the byzer project absolute path. e.g.: /opt/project/byzer-lang/
   *
   * @return
   */
  def getRootPath: String = {
    var base: String = null
    try {
      val testClassPath: String = DockerUtils.getClass.getResource("/").getPath
      val directory: File = new File(testClassPath + "../../../")
      base = directory.getCanonicalPath + "/"
    } catch {
      case e: IOException =>
        LOG.error("Can not get lib path, try to use absolute path. e:" + e.getMessage)
    }
    if (base == null) {
      base = "./"
    }
    base
  }

  /**
   * Get the byzer streamingpro-it absolute path. e.g.: /opt/project/byzer-lang/streamingpro-it/
   *
   * @return
   */
  def getCurProjectRootPath: String = {
    var base: String = null
    try {
      val testClassPath: String = DockerUtils.getClass.getResource("/").getPath
      val directory: File = new File(testClassPath + "../../")
      base = directory.getCanonicalPath + "/"
    } catch {
      case e: IOException =>
        LOG.error("Can not get lib path, try to use absolute path. e:" + e.getMessage)
    }
    if (base == null) {
      base = "./"
    }
    base
  }
}
