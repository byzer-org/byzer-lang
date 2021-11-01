package tech.mlsql.it

import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkCoreVersion
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.core.version.MLSQLVersion

import scala.collection.mutable.ListBuffer

object TestManager extends Logging {

  val nl: String = System.lineSeparator()

  var testCases: ListBuffer[TestCase] = ListBuffer()

  var failedCases: ListBuffer[(TestCase, String)] = ListBuffer()

  var matchesReg:String = ".*"

  def loadTestCase(testCaseDir: File): Unit = {
    matchesReg = Option( System.getProperty("matches") ) match {
      case Some(m) => m
      case None => matchesReg
    }
    if (testCaseDir.exists() && testCaseDir.isDirectory) {
      testCaseDir.listFiles().sortBy(_.getName).foreach(file => {
        if (file.isFile &&
          file.getName.endsWith("mlsql") &&
          file.getName.stripSuffix(".mlsql").matches(matchesReg)
        ) {
          logInfo(s"collect test file: ${file.getName}; matches=${matchesReg}")
          val expectedFileName = s"""${file.getName}.expected"""
          val expectedFile = new File(file.getParent, expectedFileName)
          val content = FileUtils.readFileToString(file)
          if (expectedFile.exists()) {
            testCases += TestCase(file.getPath, content, expectedFile)
          } else {
            testCases += TestCase(file.getPath, content, null)
          }
        }
        if (file.isDirectory) {
          loadTestCase(file)
        }
      })
    }
  }

  def clear(): Unit = {
    testCases.clear()
    failedCases.clear()
  }


  def recordError(testCase: TestCase, msg: String): Unit = {
    failedCases += Tuple2(testCase, msg)
    logInfo("========================= Error Test Case =========================")
    logInfo(s"Error: TestCase ${testCase.name} failed, error msg is: $msg")
  }


  def recordError(testCase: TestCase, t: Throwable): Unit = {
    def getRootCause(t: Throwable): String = {
      var t1 = t
      if (t1 == null) return ""
      while (t1 != null) {
        if (t1.getCause == null) {
          var msg = t1.getMessage
          if (msg == null) msg = t1.toString
          return msg
        }
        t1 = t1.getCause
      }
      t1.getMessage
    }

    recordError(testCase, getRootCause(t))
    t.printStackTrace()
  }

  def accept(testCase: TestCase, result: Seq[Seq[String]], exception: Exception): Unit = {
    val hints = testCase.getHintList
    var comparator: Comparator = DefaultComparator
    if (hints.contains("comparator")) {
      val clazzName = hints("comparator")
      try {
        comparator = Class.forName(clazzName).newInstance().asInstanceOf[Comparator]
      } catch {
        case _: ClassNotFoundException => logInfo(s"Warn: can not load comparator $clazzName, use default.")
      }
    }
    val compareResult: (Boolean, String) = comparator.compare(testCase, result, exception)
    if (!compareResult._1) {
      recordError(testCase, compareResult._2)
    }
  }


  def report(): Unit = {
    logInfo("========================= Test Result =========================")
    if (failedCases.isEmpty) {
      logInfo(s"All tests (${testCases.size} total) passed.")
      assert(true)
    } else {
      val failedCaseCount = failedCases.size
      logInfo(s"There are $failedCaseCount failed tests (${testCases.size} total), please check above.")
      var index = 1
      failedCases.foreach { failedCase => {
        val testCase: TestCase = failedCase._1
        val stackTrace = failedCase._2
        val projectName = System.getProperty("user.dir")
        var pn = projectName
        if(projectName.contains("/")){
          pn = projectName.substring(pn.lastIndexOf("/") + 1, projectName.length())
        }else if(projectName.contains("\\")){
          pn = pn.substring(projectName.lastIndexOf('\\') + 1, projectName.length())
        }
        val mlsqlVersion = MLSQLVersion.version().version
        val sparkVersion = SparkCoreVersion.exactVersion
        val scalaVersion = Option(util.Properties.versionNumberString) match {
          case Some(version) if version.nonEmpty => "-" + version
          case _ => ""
        }
        logInfo(s"Failed to execute goal ${testCase.name} on project ${pn}-${sparkVersion}${scalaVersion}:mlsql-engine_${sparkVersion}_${mlsqlVersion}                 [$index/$failedCaseCount]")
        logInfo(s"sql: ${testCase.sql}")
        logInfo(s"Here is the test failed stack trace: $stackTrace")
        index += 1
      }
      }
      System.exit(1)
    }
  }

}

case class TestCase(name: String, sql: String, expected: File) {

  def getHintList: Map[String, String] = {
    sql.split("\n")
      .filter(_.stripMargin.startsWith("--%"))
      .filter(_.contains("="))
      .map { item =>
        val Array(k, v) = item.stripMargin.stripPrefix("--%").split("=", 2)
        k -> v
      }.toMap
  }
}
