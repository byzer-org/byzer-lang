package tech.mlsql.it

import java.io.{FileReader, PrintWriter, StringWriter}

import breeze.io.CSVReader
import org.apache.commons.io.FileUtils

import scala.compat.Platform.EOL

trait Comparator {

  def of(_baseComparator: Comparator): Unit = {
  }

  def compare(testCase: TestCase, testResult: TestResult): (Boolean, String) = {
    if (testResult.hasException) {
      compareException(testCase, testResult)
    } else {
      compareResult(testCase, testResult)
    }
  }

  def compareException(testCase: TestCase, testResult: TestResult): (Boolean, String) = ???

  def compareResult(testCase: TestCase, testResult: TestResult): (Boolean, String) = ???
}

class DefaultComparator extends Comparator {

  def getExceptionStackAsString(exception: Exception): String = {
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    exception.printStackTrace(pw)
    sw.toString
  }

  override def compareException(testCase: TestCase, testResult: TestResult): (Boolean, String) = {
    val hints: Map[String, String] = testCase.getHintList
    val curTestResult = testResult.asInstanceOf[DefaultTestResult]
    if (!hints.contains("exception") || !hints.contains("msg")) {
      return (false, "\n" + getExceptionStackAsString(curTestResult.exception))
    }

    val name = hints("exception")
    val msg = hints("msg")
    try {
      val clazz = Class.forName(name)
      if (clazz.isInstance(curTestResult.exception) && curTestResult.exception.getMessage.matches(msg)) {
        return (true, "")
      }
      (false, s"\nExpected exception and message: $name, $msg\nActual exception and message: " +
        s"${curTestResult.exception.getClass.getName}, ${curTestResult.exception.getMessage}")
    } catch {
      case cnfe: ClassNotFoundException =>
        (false, s"Exception class not found: $name")
      case e: Exception =>
        (false, "\n" + getExceptionStackAsString(e))
    }
  }

  override def compareResult(testCase: TestCase, testResult: TestResult): (Boolean, String) = {
    if (testCase.expected == null) {
      return (false, "")
    }
    val curTestResult = testResult.asInstanceOf[DefaultTestResult]
    val result = curTestResult.result
    val expectContent = FileUtils.readFileToString(testCase.expected)
    val expected: Seq[Seq[String]] = CSVReader.read(new FileReader(testCase.expected), ',')
    val sb = new StringBuilder

    result.foreach(r => sb.append(r.mkString(",") + "\n"))
    val actualContent = sb.substring(0, sb.length - 1)
    val msg = s"${EOL}expect result is: ${EOL}${expectContent}${EOL}\nbut actual result is: ${EOL}${actualContent}"

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