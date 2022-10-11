package tech.mlsql.it

import java.io.{FileReader, PrintWriter, StringWriter}

import breeze.io.CSVReader
import org.apache.commons.io.FileUtils

import scala.compat.Platform.EOL

trait Comparator {

  def compare(testCase: TestCase, result: Seq[Seq[String]], exception: Exception, status: Int = 200, errorMsg: String = "{}"):
  (Boolean, String) = {
    if (exception != null || status != 200) {
      compareException(testCase, exception, errorMsg)
    } else {
      compareResult(testCase, result)
    }
  }

  def compareException(testCase: TestCase, exception: Exception, errorMsg: String): (Boolean, String) = ???

  def compareResult(testCase: TestCase, result: Seq[Seq[String]]): (Boolean, String) = ???
}


class DefaultComparator extends Comparator {

  def getExceptionStackAsString(exception: Exception): String = {
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    exception.printStackTrace(pw)
    sw.toString
  }

  override def compareException(testCase: TestCase, exception: Exception, errorMsg: String): (Boolean, String) = {
    val hints: Map[String, String] = testCase.getHintList
    if (!hints.contains("exception") || !hints.contains("msg")) {
      if (exception == null) {
        return (false, "\n" + errorMsg)
      }
      return (false, "\n" + getExceptionStackAsString(exception))
    }

    val exceptionClassName = hints("exception")
    val msg = hints("msg")
    try {
      val clazz = Class.forName(exceptionClassName)
      if (exception == null){
        if (errorMsg.contains(exceptionClassName) && errorMsg.contains(msg)) {
          return (true, "")
        }
        return (false, s"\nExpected exception and message: $exceptionClassName, $msg\nActual exception name and message: " +
          errorMsg)
      }

      if (clazz.isInstance(exception) && exception.getMessage.matches(msg)) {
        return (true, "")
      }
      (false, s"\nExpected exception and message: $exceptionClassName, $msg\nActual exception name and message: " +
        s"${exception.getClass.getName}, ${exception.getMessage}")
    } catch {
      case cnfe: ClassNotFoundException =>
        (false, s"Exception class not found: $exceptionClassName")
      case e: Exception =>
        (false, "\n" + getExceptionStackAsString(e))
    }
  }

  override def compareResult(testCase: TestCase, result: Seq[Seq[String]]): (Boolean, String) = {
    if (testCase.expected == null) {
      return (false, "")
    }
    val expectContent = FileUtils.readFileToString(testCase.expected)
    val expected: Seq[Seq[String]] = CSVReader.read(new FileReader(testCase.expected), ',')
    val sb = new StringBuilder

    result.foreach(_.addString(sb, "", ",", "\n"))
    val actualContent = sb.substring(0, sb.length - 1)
    val msg = s"${EOL}expect result is: ${EOL}${expectContent}${EOL}but actual result is ${EOL}${actualContent}"

    if (expected.length != result.length) {
      return (false, msg)
    }

    for (i <- result.indices) {
      val actualRow = result(i)
      val expectedRow = expected(i)
      if (actualRow.length != expectedRow.length) {
        return (false, msg)
      }
      for (j <- actualRow.indices) {
        val actualVal = actualRow(j)
        val expectedVal = expectedRow(j)
        if (!actualVal.matches(expectedVal)) {
          return (false, msg)
        }
      }
    }

    (true, "")
  }
}

object DefaultComparator extends DefaultComparator {}