package tech.mlsql.dsl.includes

import java.io.{File, PrintWriter}

import org.apache.spark.sql.SparkSession
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.lib.TextProgressMonitor
import streaming.dsl.{IncludeSource, ScriptSQLExec}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.path.PathFun

/**
 * 6/5/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class LibIncludeSource extends IncludeSource with Logging {
  override def fetchSource(sparkSession: SparkSession, path: String, options: Map[String, String]): String = {
    val context = ScriptSQLExec.context()

    var libValue = path
    val commitValue = options.getOrElse("commit", "")
    val aliasValue = options.getOrElse("alias", "")

    //"./__mlsql__/deps/"
    val targetPath = PathFun(".").add("__mlsql__").add("deps").toPath
    val targetFile = new File(targetPath)
    if (!targetFile.exists()) {
      targetFile.mkdirs()
    }
    if (!libValue.endsWith(".git")) {
      libValue += ".git"
    }
    if (!libValue.startsWith("http")) {
      libValue = "https://" + libValue
    }
    val projectName = libValue.split("/").last.stripSuffix(".git")
    val finalProjectPath = PathFun(targetPath).add(projectName).toPath

    if (!new File(finalProjectPath).exists()) {
      val repository = Git.cloneRepository().setURI(libValue).setTimeout(60*5).setDirectory(new File(finalProjectPath)).
        setProgressMonitor(new TextProgressMonitor(new PrintWriter(System.out))).call()
      if (commitValue != null && !commitValue.isEmpty) {
        repository.checkout().setCreateBranch(true).setName(commitValue).setStartPoint(commitValue).call()
      }
      repository.close()
    }

    context.execListener.env().put(s"__lib__${aliasValue}", finalProjectPath)
    logInfo(s"Clone project ${libValue} alias ${aliasValue} to ${finalProjectPath} ")
    ""
  }

  override def skipPathPrefix: Boolean = true
}
