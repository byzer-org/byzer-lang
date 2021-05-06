package tech.mlsql.dsl.includes

import java.io.File

import org.apache.spark.sql.SparkSession
import streaming.dsl.{IncludeSource, ScriptSQLExec}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.path.PathFun

import scala.io.Source

/**
 * 6/5/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class ScriptIncludeSource extends IncludeSource with Logging {
  override def fetchSource(sparkSession: SparkSession, path: String, options: Map[String, String]): String = {
    val context = ScriptSQLExec.context()
    val pathChunk = if (path.contains("/")) {
      val temp = path.split("/")
      val header = temp.dropRight(1) ++ Array(temp.last.split("\\.").head)
      Array(header.mkString("/")) ++ temp.last.split("\\.").drop(1)

    } else {
      path.split("\\.")
    }
    val libAlias = pathChunk.head

    var libPath = context.execListener.env().get(s"__lib__${libAlias}")

    if (!libPath.isDefined) {
      val Array(website, user, repo) = libAlias.split("/")
      val projectPath = PathFun("/").add("tmp").add("__mlsql__").add("deps").add(website).add(user).add(repo).toPath
      val dep = new File(projectPath)
      if (dep.exists()) {
        libPath = Option(projectPath)
      } else {
        throw new RuntimeException(s"Lib ${libAlias} is not imported. This may caused by fail to download the project.")
      }
    }
    val rootPath = PathFun(libPath.get)

    pathChunk.drop(1).foreach { item =>
      rootPath.add(item)
    }
    var finalPath = rootPath.toPath
    if (!finalPath.endsWith(".mlsql")) {
      finalPath += ".mlsql"
    }

    Source.fromFile(finalPath).getLines().mkString("\n")
  }

  override def skipPathPrefix: Boolean = true
}

