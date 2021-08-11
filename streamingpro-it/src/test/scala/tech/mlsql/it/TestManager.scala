package tech.mlsql.it

import java.io.File

import org.apache.commons.io.FileUtils

import scala.collection.mutable.ListBuffer

object TestManager {

  val nl: String = System.lineSeparator()

  var testCases: ListBuffer[TestCase] = ListBuffer()

  var failedCases: ListBuffer[(TestCase, String)] = ListBuffer()

  def loadTestCase(testCaseDir: File): Unit = {
    if (testCaseDir.exists() && testCaseDir.isDirectory) {
      testCaseDir.listFiles().sortBy(_.getName).foreach(file => {
        if (file.isFile && file.getName.endsWith("mlsql")) {
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
    println("========================= Error Test Case =========================")
    println(s"Error: TestCase ${testCase.name} failed, error msg is: $msg")
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
        case _: ClassNotFoundException => println(s"Warn: can not load comparator $clazzName, use default.")
      }
    }
    val compareResult: (Boolean, String) = comparator.compare(testCase, result, exception)
    if (!compareResult._1) {
      recordError(testCase, compareResult._2)
    }
  }


  def report(): Unit = {
    println("========================= Test Result =========================")
    if(failedCases.isEmpty) {
      println(s"All tests (${testCases.size} total) passed.")
      assert(true)
    } else {
      println(s"There are ${failedCases.size} failed tests (${testCases.size} total), please check above.")
      assert(false)
    }
  }

}

case class TestCase(name: String, sql: String, expected: File) {

  def getHintList: Map[String, String] = {
    sql.split("\n")
      .filter(_.stripMargin.startsWith("--%"))
      .filter(_.contains("="))
      .map{ item =>
        val Array(k, v) = item.stripMargin.stripPrefix("--%").split("=", 2)
        k -> v
      }.toMap
  }
}
