package tech.mlsql.dsl.adaptor

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.mlsql.session.MLSQLException
import streaming.common.JSONTool
import streaming.dsl.parser.DSLSQLParser
import streaming.dsl.parser.DSLSQLParser._
import streaming.dsl.template.TemplateMerge
import tech.mlsql.dsl.processor.PreProcessListener

import scala.collection.mutable.ArrayBuffer

/**
  * 2019-04-11 WilliamZhu(allwefantasy@gmail.com)
  */
class CommandAdaptor(preProcessListener: PreProcessListener) extends DslAdaptor {

  def evaluate(str: String) = {
    TemplateMerge.merge(str, preProcessListener.scriptSQLExecListener.env().toMap)
  }

  def analyze(ctx: SqlContext): CommandStatement = {
    var command = ""
    var parameters = ArrayBuffer[String]()
    command = ctx.getChild(0).getText.substring(1)


    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: CommandValueContext =>
          val oringinalText = s.getText
          parameters += cleanBlockStr(cleanStr(evaluate(oringinalText)))
        case s: RawCommandValueContext =>
          val box = ArrayBuffer[String]()
          var prePos = 0
          (0 to s.getChildCount() - 1).foreach { itokenIndex =>
            val child = s.getChild(itokenIndex)
            if (itokenIndex == 0 || (child.getSourceInterval.a - prePos == 1)) {
              box += child.getText
            } else {
              parameters += box.mkString("")
              box.clear()
              box += child.getText
            }
            prePos = child.getSourceInterval.b
          }

          parameters += box.mkString("")
          box.clear()

        case _ =>
      }
    }
    CommandStatement(currentText(ctx), command, parameters.toList)
  }

  override def parse(ctx: DSLSQLParser.SqlContext): Unit = {
    val CommandStatement(_, _command, _parameters) = analyze(ctx)
    val command = _command
    val parameters = new ArrayBuffer[String]() ++ _parameters
    val env = preProcessListener.scriptSQLExecListener.env()
    val tempCommand = try env(command) catch {
      case e: java.util.NoSuchElementException =>
        throw new MLSQLException(s"Command `${command}` is not found.")
      case e: Exception => throw e
    }
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

    preProcessListener.addStatement(String.valueOf(finalCommand.toArray))

  }
}

case class CommandStatement(raw: String, command: String, parameters: List[String])
