package tech.mlsql.dsl.includes

import java.io.File

import org.apache.spark.sql.SparkSession
import streaming.dsl.{IncludeSource, ScriptSQLExec}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.session.SetSession

import scala.io.Source

/**
 * 6/5/2021 WilliamZhu(allwefantasy@gmail.com)
 * This is work with LibIncludeSource
 * It will try to load the lib from local which is downloaded by LibIncludeSource
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

    var libPath: Option[String] = None
    val envSession = new SetSession(sparkSession, context.owner)
    val libPathOpt = envSession.fetchSetStatement.map { item => item.collect().filter(setItem => setItem.k == s"__lib__${libAlias}").headOption }
    if (libPathOpt.isDefined && libPathOpt.get.isDefined) {
      libPath = libPathOpt.get.map(item => item.v)
    }

    /**
     * if we can not find the lib name from session, we will try to find it from local.
     * however, this require the libAlias is a full name.
     * for example:
     * include local.`gitee.com/allwefantasy/lib-core.udf.hello`
     */
    if (!libPath.isDefined) {
      val Array(website, user, repo) = libAlias.split("/")
      val projectPath = PathFun.home.add(".mlsql").add("deps").add(website).add(user).add(repo).toPath
      val dep = new File(projectPath)
      if (dep.exists()) {
        libPath = Option(projectPath)
      } else {
        throw new RuntimeException(s"Lib ${libAlias} is not imported. This may caused by fail to download the project.")
      }
    }
    val rootPath = PathFun(libPath.get)
    var suffix = "mlsql"
    var newPathChunk = pathChunk

    pathChunk.last.toLowerCase match {
      case "mlsql" =>
        newPathChunk = newPathChunk.dropRight(1)
      case "byzer" | "byz" | "by" | "py" =>
        newPathChunk = newPathChunk.dropRight(1)
        suffix = pathChunk.last
      case _ =>


    }

    newPathChunk.drop(1).foreach { item =>
      rootPath.add(item)
    }

    val finalPath = rootPath.toPath + "." + suffix

    val action = options.getOrElse("__action__", "set")
    val leftParams = options - "__action__"

    val generateCode = action match {
      case "set" => leftParams.map { item =>
        s"""set ${item._1}='''${item._2}''';"""
      }.mkString("\n")
      case _ => ""
    }

    generateCode + Source.fromFile(finalPath).getLines().mkString("\n")
  }

  override def skipPathPrefix: Boolean = true
}