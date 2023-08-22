package tech.mlsql.dsl.includes

import java.io.{File, PrintWriter}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.lib.TextProgressMonitor
import org.eclipse.jgit.storage.file.FileRepositoryBuilder
import streaming.dsl.{IncludeSource, ScriptSQLExec}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.session.SetSession

/**
 * 6/5/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class LibIncludeSource extends IncludeSource with Logging {
  override def fetchSource(sparkSession: SparkSession, path: String, options: Map[String, String]): String = {
    val context = ScriptSQLExec.context()

    var libValue = path
    if (libValue.startsWith("https")) {
      throw new IllegalArgumentException("https:// prefix is not allowed.")
    }

    if (libValue.endsWith(".git")) {
      throw new IllegalArgumentException(".git suffix is not allowed.")
    }

    val Array(website, user, repo) = libValue.split("/")
    val libMirror = options.getOrElse("libMirror", website)

    val commitValue = options.getOrElse("commit", "")
    val aliasValue = options.getOrElse("alias", "")
    val force = options.getOrElse("force", "false")
    val onlyForCurrentInstance = options.getOrElse("onlyForCurrentInstance", "false").toBoolean

    //"./__mlsql__/deps/"
    var targetPath = PathFun.home.add(".mlsql").add("deps").toPath
    if(onlyForCurrentInstance) {
      targetPath = PathFun.current.add(".mlsql").add("deps").toPath
    }
    val targetFile = new File(targetPath)
    if (!targetFile.exists()) {
      targetFile.mkdirs()
    }

    libValue = s"https://${libMirror}/${user}/${repo}.git"

    val finalProjectPath = PathFun(targetPath).add(website).add(user).add(repo).toPath

    def cloneRepo = {

      val repository = Git.cloneRepository().setURI(libValue).setTimeout(60 * 5).setDirectory(new File(finalProjectPath)).
        setProgressMonitor(new TextProgressMonitor(new PrintWriter(System.out))).call()
      if (commitValue != null && !commitValue.isEmpty) {
        repository.checkout().setCreateBranch(true).setName(commitValue).setStartPoint(commitValue).call()
      }
      repository.close()
    }

    if (!new File(finalProjectPath).exists() || force.toBoolean) {
      FileUtils.deleteQuietly(new File(finalProjectPath))
      cloneRepo
    } else {
      val repositoryBuilder = new FileRepositoryBuilder
      val repository = repositoryBuilder.setGitDir(new File(finalProjectPath)).build()
      if (!commitValue.isEmpty && repository.getBranch != commitValue) {
        FileUtils.deleteQuietly(new File(finalProjectPath))
        cloneRepo
      }
    }
    if (!new File(PathFun(finalProjectPath).add("package.json").toPath).exists()) {
      FileUtils.deleteQuietly(new File(finalProjectPath))
      throw new IllegalArgumentException(s"${libValue} is not a valid MLSQL project.")
    }
    val envSession = new SetSession(sparkSession, context.owner)
    envSession.set(s"__lib__${aliasValue}", finalProjectPath, Map(SetSession.__MLSQL_CL__ -> SetSession.SET_STATEMENT_CL))
    //    context.execListener.env().put(s"__lib__${aliasValue}", finalProjectPath)
    logInfo(s"Clone project ${libValue} alias ${aliasValue} to ${finalProjectPath} ")
    "run command as EmptyTable.``;"
  }

  override def skipPathPrefix: Boolean = true
}
