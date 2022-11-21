package tech.mlsql.it

import breeze.io.CSVReader
import com.google.gson.{Gson, JsonObject, JsonParser}
import org.apache.commons.io.FileUtils

import java.io.{FileReader, PrintWriter, StringWriter}
import java.util
import scala.compat.Platform.EOL

/**
 * 13/10/2022 hellozepp(lisheng.zhanglin@163.com)
 */
class RestComparator extends Comparator {

  override def compareException(testCase: TestCase, testResult: TestResult): (Boolean, String) = {
    val curTestResult = testResult.asInstanceOf[RestTestResult]
    val errorMsg = curTestResult.result
    val exception = curTestResult.exception
    val hints: Map[String, String] = testCase.getHintList
    if (!hints.contains("exception") || !hints.contains("msg")) {
      if (exception == null) {
        return (false, "\n" + errorMsg)
      }
      return (false, "\n" + getExceptionStackAsString(exception))
    }

    val exceptionClassName = hints("exception")
    val exceptionClassNameHelper = exceptionClassName.r
    val msg = hints("msg")
    val msgHelper = msg.r
    try {
      val clazz = Class.forName(exceptionClassName)
      //exception class is null, use errorMessage for matching. This method acts on the result returned by rest in yarn mode.
      if (exception == null) {
        if ((exceptionClassNameHelper findFirstIn errorMsg).isDefined && (msgHelper findFirstIn errorMsg).isDefined) {
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

  def getExceptionStackAsString(exception: Exception): String = {
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    exception.printStackTrace(pw)
    sw.toString
  }

  override def compareResult(testCase: TestCase, testResult: TestResult): (Boolean, String) = {
    if (testCase.expected == null) {
      return (false, "")
    }

    val expectContent = FileUtils.readFileToString(testCase.expected)
    val expected: Seq[Seq[String]] = CSVReader.read(new FileReader(testCase.expected), ',')
    val sb = new StringBuilder
    var resultSeq = Seq[Seq[String]]()
    val curTestResult = testResult.asInstanceOf[RestTestResult]
    val result = curTestResult.result

    if (result != null && result.nonEmpty) {
      val returnData = new JsonParser().parse(result).getAsJsonObject
      var resultTitle = Seq[String]()
      import scala.collection.JavaConversions._
      if (returnData != null && returnData.size() > 0) {
        if (returnData.has("schema") && returnData.getAsJsonObject("schema")
          .has("fields")) {
          val fields = returnData.getAsJsonObject("schema").getAsJsonArray("fields")
          if (fields != null) {
            val resultTitleHelper = new util.ArrayList[String]()
            fields.foreach(ele => {
              if (ele.getAsJsonObject != null && ele.getAsJsonObject.has("name")) {
                resultTitleHelper.add(ele.getAsJsonObject.get("name").getAsString)
              }
            })
            resultTitle = resultTitleHelper.toSeq
          }
        }

        if (returnData.has("data")) {
          val dataArr = returnData.getAsJsonArray("data")
          if (dataArr != null && dataArr.nonEmpty) {
            resultSeq = dataArr.map(obj => {
              val oneDataObj = obj.getAsJsonObject
              if (oneDataObj != null && oneDataObj.size != 0) {
                val resultHelper = new util.ArrayList[String]()
                for (i <- resultTitle.indices) {
                  val title = resultTitle(i)
                  val v = oneDataObj.get(title)
                  if (v != null) {
                    resultHelper.add(v.getAsString)
                  } else {
                    resultHelper.add("null")
                  }
                }
                resultHelper.toSeq
              } else {
                Seq[String]()
              }
            }).toSeq
            resultSeq = resultTitle +: resultSeq
            resultSeq.foreach(r => sb.append(r.mkString(",") + "\n"))
          }
        }
      }
    }

    val actualContent = sb.substring(0, sb.length - 1)
    val msg = s"${EOL}result length is: ${EOL}${expected.length}${EOL}\nactual result length is: ${EOL}${resultSeq.length}," +
      s"${EOL}expect result is: ${EOL}${expectContent}${EOL}but actual result is: ${EOL}${actualContent}"

    if (expected.length != resultSeq.length) {
      return (false, msg)
    }

    for (i <- resultSeq.indices) {
      val actualRow = resultSeq(i)
      val expectedRow = expected(i)
      if (actualRow.length != expectedRow.length) {
        // Compare empty lines separately
        if (!(actualRow.isEmpty && expectedRow.length == 1 && expectedRow.head.isEmpty)) {
          return (false, msg)
        }
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

object RestComparator extends RestComparator {}