package streaming.common

import scala.collection.mutable.ArrayBuffer

/**
  * 2019-04-25 WilliamZhu(allwefantasy@gmail.com)
  */
class PathFun(rootPath: String) {
  private val buffer = new ArrayBuffer[String]()
  buffer += rootPath.stripSuffix("/")
  
  def add(path: String) = {
    val cleanPath = path.stripPrefix("/").stripSuffix("/")
    if (!cleanPath.isEmpty) {
      buffer += cleanPath
    }
    this
  }

  def /(path: String) = {
    add(path)
  }

  def toPath = {
    buffer.mkString("/")
  }

}

object PathFun {
  def apply(rootPath: String): PathFun = new PathFun(rootPath)

  def joinPath(rootPath: String, paths: String*) = {
    val pf = apply(rootPath)
    for (arg <- paths) pf.add(arg)
    pf.toPath
  }
}
