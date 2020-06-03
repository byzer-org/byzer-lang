package tech.mlsql.atuosuggest.statement

import org.antlr.v4.runtime.Token
import org.antlr.v4.runtime.misc.Interval
import org.apache.spark.sql.catalyst.parser.SqlBaseLexer
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.atuosuggest.meta.MetaTableKey
import tech.mlsql.atuosuggest.{AutoSuggestContext, TokenPos}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * 3/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class SelectSuggester(val context: AutoSuggestContext, val tokens: List[Token], val tokenPos: TokenPos) extends StatementSuggester with SuggesterRegister {

  private val subInstances = new mutable.HashMap[String, StatementSuggester]()

  override def name: String = "select"

  override def isMatch(): Boolean = {
    tokens.headOption.map(_.getType) match {
      case Some(DSLSQLLexer.SELECT) => true
      case _ => false
    }
  }

  override def suggest(): List[SuggestItem] = {
    val start = tokens.head.getStartIndex
    val stop = tokens.last.getStopIndex

    val input = tokens.head.getTokenSource.asInstanceOf[DSLSQLLexer]._input
    val interval = new Interval(start, stop)
    val originalText = input.getText(interval)
    val newTokens = context.rawSQLLexer.tokenizeNonDefaultChannel(originalText)

    newTokens.tokens.asScala.map(item => s"${item.toString}  type:${item.getType}").toList.map(SuggestItem(_))
  }

  override def register(clzz: Class[_ <: StatementSuggester]): SuggesterRegister = {
    val instance = clzz.getConstructor(classOf[SelectSuggester]).newInstance(this).asInstanceOf[StatementSuggester]
    subInstances.put(instance.name, instance)
    this
  }
}

case class MetaTableKeyWrapper(metaTableKey: MetaTableKey, aliasName: String)

/**
 * the atom statement is only contains:
 * select,from,groupby,where,join limit
 * Notice that we do not make sure the sql is right
 *
 */
class SingleStatementAST(selectSuggester: SelectSuggester, start: Int, stop: Int) {
  val children = ArrayBuffer[SingleStatementAST]()

  def printAsStr(_tokens: List[Token]): String = {
    val tokens = _tokens.slice(start, stop)
    val stringBuilder = new mutable.StringBuilder()
    stringBuilder.append(tokens.map(_.getText).mkString(" "))
    stringBuilder.append("\n")
    children.zipWithIndex.foreach { case (item, index) => stringBuilder.append("=" * (index + 1) + ">" + item.printAsStr(tokens)) }
    stringBuilder.toString()
  }
}

object SingleStatementAST {
  def build(selectSuggester: SelectSuggester, tokens: List[Token], start: Int, stop: Int, isSub: Boolean = false): SingleStatementAST = {
    val ROOT = new SingleStatementAST(selectSuggester, 0, stop)
    // context start: ( select
    // context end: )

    var bracketStart = 0
    for (index <- (start until stop)) {
      val token = tokens(index)

      if (token.getType == SqlBaseLexer.T__0 && index < stop - 1 && tokens(index + 1).getType == SqlBaseLexer.SELECT) {
        println(index)
        ROOT.children += SingleStatementAST.build(selectSuggester, tokens, index + 1, stop, true)
      } else {
        if (isSub) {
          if (token.getType == SqlBaseLexer.T__0) {
            bracketStart += 1
            //println(s"index:[$index] bracket +1 = ${bracketStart}")
          }
          if (token.getType == SqlBaseLexer.T__1 && bracketStart != 0) {
            bracketStart -= 1
            //println(s"index:[$index] bracket -1 = ${bracketStart}")
          }
          else if (token.getType == SqlBaseLexer.T__1 && bracketStart == 0) {
            // check the alias
            return new SingleStatementAST(selectSuggester, start, index)
          }

        } else {
          // do nothing
        }
      }
    }
    ROOT
  }
}

class ProjectSuggester(selectSuggester: SelectSuggester) extends StatementSuggester with SuggesterRegister {

  val tokens = selectSuggester.tokens
  val tokenPos = selectSuggester.tokenPos
  val fromTableInCurrentScope = ArrayBuffer[MetaTableKeyWrapper]()

  override def name: String = "project"

  override def isMatch(): Boolean = {
    //make sure the pos  after select and [before from or is the end of the statement]
    // it's ok if we are wrong.
    tokens.zipWithIndex.filter(_._1.getType == SqlBaseLexer.FROM).headOption match {
      case Some(from) => from._2 > tokenPos.pos
      case None => true
    }

  }

  override def suggest(): List[SuggestItem] = {
    // try to get all information from all statement.
    ???
  }

  override def register(clzz: Class[_ <: StatementSuggester]): SuggesterRegister = ???
}

class FromSuggester

class GroupSuggester

class HavingSuggester

class SubQuerySuggester

class JoinSuggester

class FunctionSuggester



