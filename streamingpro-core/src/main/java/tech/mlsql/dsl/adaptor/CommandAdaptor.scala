package tech.mlsql.dsl.adaptor

import org.apache.spark.sql.mlsql.session.MLSQLException
import streaming.dsl.parser.DSLSQLParser
import streaming.dsl.parser.DSLSQLParser._
import streaming.dsl.template.TemplateMerge
import tech.mlsql.common.utils.base.Templates
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
    val renderContent = Templates.evaluate(tempCommand, parameters)
    preProcessListener.addStatement(renderContent)

  }
}

case class CommandStatement(raw: String, command: String, parameters: List[String])
