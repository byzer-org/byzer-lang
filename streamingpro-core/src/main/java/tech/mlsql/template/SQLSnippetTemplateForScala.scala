package tech.mlsql.template

import java.util.concurrent.atomic.AtomicInteger

import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.utils.serder.json.JSONTool

import scala.collection.mutable.ArrayBuffer

/**
 * 11/3/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class SQLSnippetTemplateForScala {
  def get(templateName: String, parameters: Array[String]): String = {
    val templateContent = ScriptSQLExec.context().execListener.env()(templateName)
    evaluate(templateContent, parameters)
  }

  def evaluate(tempCommand: String, parameters: Seq[String]) = {
    var finalCommand = ArrayBuffer[Char]()
    val len = tempCommand.length

    def fetchParam(index: Int) = {
      if (index < parameters.length) {
        parameters(index).toCharArray
      } else {
        Array[Char]()
      }
    }


    val posCount = new AtomicInteger(0)
    val curPos = new AtomicInteger(0)

    def positionReplace(i: Int): Boolean = {
      if (tempCommand(i) == '{' && i < (len - 1) && tempCommand(i + 1) == '}') {
        finalCommand ++= fetchParam(posCount.get())
        curPos.set(i + 2)
        posCount.addAndGet(1)
        return true
      }
      return false
    }

    def namedPositionReplace(i: Int): Boolean = {

      if (tempCommand(i) != '{') return false

      val startPos = i
      var endPos = i


      // now , we should process with greedy until we meet '}'
      while (endPos < len - 1 && tempCommand(endPos) != '}') {
        endPos += 1
      }

      if (startPos - 1 >= 0 && tempCommand(startPos - 1) == '$') {
        return false
      }

      val shouldBeNumber = tempCommand.slice(startPos + 1, endPos).trim
      val namedPos = try {
        Integer.parseInt(shouldBeNumber)
      } catch {
        case e: Exception =>
          return false
      }

      finalCommand ++= fetchParam(namedPos)
      curPos.set(endPos + 1)
      return true
    }

    def textEvaluate = {
      (0 until len).foreach { i =>

        if (curPos.get() > i) {
        }
        else if (positionReplace(i)) {
        }
        else if (namedPositionReplace(i)) {

        } else {
          finalCommand += tempCommand(i)
        }
      }
    }

    if (tempCommand.contains("{:all}")) {
      finalCommand ++= tempCommand.replace("{:all}", JSONTool.toJsonStr(parameters)).toCharArray
    } else {
      textEvaluate
    }
    String.valueOf(finalCommand.toArray)
  }


}
