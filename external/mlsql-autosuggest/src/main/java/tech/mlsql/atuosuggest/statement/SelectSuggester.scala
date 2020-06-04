package tech.mlsql.atuosuggest.statement

import org.antlr.v4.runtime.Token
import org.antlr.v4.runtime.misc.Interval
import org.apache.spark.sql.catalyst.parser.SqlBaseLexer
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.atuosuggest.dsl.{Food, TokenMatcher}
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
 * the atom query statement is only contains:
 * select,from,groupby,where,join limit
 * Notice that we do not make sure the sql is right
 *
 */
class SingleStatementAST(selectSuggester: SelectSuggester, var start: Int, var stop: Int, var parent: SingleStatementAST) {
  val children = ArrayBuffer[SingleStatementAST]()

  def isLeaf = {
    children.length == 0
  }

  def output(tokens: List[Token]): Unit = {
    if (isLeaf) {
      //collect table first
      // T__3 == .
      val tokenMatch = TokenMatcher(tokens.slice(0, stop), start).asStart(Food(None, SqlBaseLexer.FROM), 1).
        eat(Food(None, SqlBaseLexer.IDENTIFIER), Food(None, SqlBaseLexer.T__3)).optional.
        eat(Food(None, SqlBaseLexer.IDENTIFIER)).optional.
        build
      println(tokens(tokenMatch.get))
      println(tokens(tokenMatch.get + 1))
      if (tokenMatch.isSuccess) {
        println(tokens.slice(tokenMatch.start, tokenMatch.get))
      }

    }
    children.foreach(_.output(tokens))

  }

  def printAsStr(_tokens: List[Token], _level: Int): String = {
    val tokens = _tokens.slice(start, stop)
    val stringBuilder = new mutable.StringBuilder()
    var count = 1
    stringBuilder.append(tokens.map { item =>
      count += 1
      val suffix = if (count % 20 == 0) "\n" else ""
      item.getText + suffix
    }.mkString(" "))
    stringBuilder.append("\n")
    stringBuilder.append("\n")
    stringBuilder.append("\n")
    children.zipWithIndex.foreach { case (item, index) => stringBuilder.append("=" * (_level + 1) + ">" + item.printAsStr(_tokens, _level + 1)) }
    stringBuilder.toString()
  }
}


object SingleStatementAST {

  def matchTableAlias(tokens: List[Token], start: Int) = {
    tokens(start)
  }

  def build(selectSuggester: SelectSuggester, tokens: List[Token], start: Int, stop: Int, isSub: Boolean = false): SingleStatementAST = {
    val ROOT = new SingleStatementAST(selectSuggester, start, stop, null)
    // context start: ( select
    // context end: )

    var bracketStart = 0
    var jumpIndex = -1
    for (index <- (start until stop) if index >= jumpIndex) {
      val token = tokens(index)

      if (token.getType == SqlBaseLexer.T__0 && index < stop - 1 && tokens(index + 1).getType == SqlBaseLexer.SELECT) {
        val item = SingleStatementAST.build(selectSuggester, tokens, index + 1, stop, true)
        jumpIndex = item.stop
        ROOT.children += item
        item.parent = ROOT

      } else {
        if (isSub) {
          if (token.getType == SqlBaseLexer.T__0) {
            bracketStart += 1
          }
          if (token.getType == SqlBaseLexer.T__1 && bracketStart != 0) {
            bracketStart -= 1
          }
          else if (token.getType == SqlBaseLexer.T__1 && bracketStart == 0) {
            // check the alias
            val matcher = TokenMatcher(tokens, index + 1).eat(Food(None, SqlBaseLexer.AS)).optional.eat(Food(None, SqlBaseLexer.IDENTIFIER)).build
            val stepSize = if (matcher.isSuccess) matcher.get else index
            ROOT.start = start
            ROOT.stop = stepSize
            return ROOT
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



