package tech.mlsql.dsl.includes

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
    val pathChunk = path.split("\\.")
    val libAlias = pathChunk.head

    val libPath = context.execListener.env().get(s"__lib__${libAlias}")
    if (!libPath.isDefined) {
      throw new RuntimeException(s"Lib ${libAlias} is not imported")
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

