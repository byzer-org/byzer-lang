package tech.mlsql.ets.hdfs

import org.apache.hadoop.fs.{FileSystem, Path}
import streaming.core.datasource.FSConfig
import tech.mlsql.common.utils.lang.goland.{defer, goScope}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.common.utils.shell.command.ParamsUtil
import tech.mlsql.log.LogUtils

import java.io.{BufferedReader, InputStreamReader}

/**
 * 11/8/2022 WilliamZhu(allwefantasy@gmail.com)
 */
class FSGetmerge(fsConf: FSConfig, args: List[String]) extends Logging {

  private def merge(fs: FileSystem, source: String, target: String, skipNLines: Int) = {
    val newTarget = PathFun(fsConf.path).add(target).toPath

    val srcs = source.split(",").flatMap { path =>
      val newPath = PathFun(fsConf.path).add(path).toPath
      val srcs = fs.globStatus(new Path(newPath))
      srcs
    }

    val targetPath = new Path(newTarget)
    require(!fs.exists(targetPath), s"${newTarget} already exists")

    val dos = fs.create(targetPath, true)
    try {
      srcs.zipWithIndex.foreach { case (src, index) =>
        goScope {
          val br = new BufferedReader(new InputStreamReader(fs.open(src.getPath)))
          defer {
            br.close()
          }
          var empty = false
          if (index > 0) {
            var count = skipNLines
            while (count > 0) {
              val line = br.readLine()
              if (line == null) {
                count = 0
                empty = true
              } else {
                count -= 1
              }
            }
          }

          var line = br.readLine()
          while (!empty && line != null) {
            dos.writeBytes(line + "\n")
            line = br.readLine()
          }
        }

      }
    } finally {
      dos.close()
    }
  }

  def run: (Boolean, String) = {
    val fs = FileSystem.get(fsConf.conf)

    args.headOption match {
      case Some("_") =>
        val parser = new ParamsUtil(args.drop(1).mkString(" "))

        val source = parser.getParam("source")
        val target = parser.getParam("target")
        val skipNLines = parser.getParam("skipNLines", "0").toInt

        require(source != null && target != null, "-source and -target should be specified")
        try {
          merge(fs, source, target, skipNLines)
          (true, "SUCCESS")
        } catch {
          case e: Exception =>
            logError("Fail to merge files:", e)
            (false, LogUtils.format_exception(e))
        }

      case _ =>
        val source = args.dropRight(2).mkString(",")
        val (target, skipNLines) = (args(args.length - 2), args(args.length - 1))
        try {
          merge(fs, source, target, skipNLines.toInt)
          (true, "SUCCESS")
        } catch {
          case e: Exception =>
            logError("Fail to merge files:", e)
            (false, LogUtils.format_exception(e))
        }

    }

  }
}
