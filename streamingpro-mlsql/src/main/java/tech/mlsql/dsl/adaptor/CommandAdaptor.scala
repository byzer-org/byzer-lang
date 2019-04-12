package tech.mlsql.dsl.adaptor

import streaming.dsl.DslAdaptor
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

  override def parse(ctx: DSLSQLParser.SqlContext): Unit = {
    var command = ""
    var parameters = ArrayBuffer[String]()
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: CommandContext =>
          command = s.getText.substring(1)
        case s: SetValueContext =>
          var oringinalText = s.getText
          parameters += cleanBlockStr(cleanStr(evaluate(oringinalText)))
        case s: SetKeyContext =>
          parameters += s.getText
        case _ =>
      }
    }
    val env = preProcessListener.scriptSQLExecListener.env()
    val tempCommand = env(command)
    var finalCommand = ArrayBuffer[Char]()
    val len = tempCommand.length

    def fetchParam(index: Int) = {
      if (index < parameters.length) {
        parameters(index).toCharArray
      } else {
        Array[Char]()
      }

    }

    if (parameters.size > 0) {
      var count = 0
      (0 until len).foreach { i =>
        if (tempCommand(i) == '{' && i < (len - 1) && tempCommand(i + 1) == '}') {
          finalCommand ++= fetchParam(count)
          count += 1
        } else if (i >= 1 && tempCommand(i - 1) == '{' && tempCommand(i) == '}') {
          //
        }
        else {
          finalCommand += tempCommand(i)
        }

      }
    }

    preProcessListener.addStatement(String.valueOf(finalCommand.toArray))

  }
}
